import logging

import httpx
from faststream import Context, ContextRepo, FastStream
from faststream.rabbit import RabbitMessage

from app.config import settings
from app.db import engine, session_factory
from app.messaging import (
    ATTEMPT_HEADER,
    create_broker,
    dead_letter_exchange,
    dead_letter_queue,
    declare_topology,
    payments_exchange,
    payments_queue,
    retry_queue,
)
from app.processing import GatewayEmulator, PaymentNotFound, PaymentProcessor, WebhookNotifier
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)

broker = create_broker()
app = FastStream(broker)


@app.on_startup
async def startup(context: ContextRepo) -> None:
    await broker.connect()
    await declare_topology(broker)

    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    gateway = GatewayEmulator(
        settings.gateway_success_rate,
        settings.gateway_min_delay,
        settings.gateway_max_delay,
    )
    context.set_global("http_client", http_client)
    context.set_global("processor", PaymentProcessor(session_factory, gateway, WebhookNotifier(http_client)))


@app.after_shutdown
async def shutdown(http_client: httpx.AsyncClient = Context()) -> None:
    await http_client.aclose()
    await engine.dispose()


@broker.subscriber(payments_queue, payments_exchange)
async def handle_new_payment(
    event: PaymentEvent,
    message: RabbitMessage,
    processor: PaymentProcessor = Context(),
) -> None:
    attempt = int(message.headers.get(ATTEMPT_HEADER, 1))

    try:
        await processor.process(event.payment_id)
    except PaymentNotFound as exc:
        await dead_letter(message, attempt, exc)
    except Exception as exc:
        if attempt >= settings.max_attempts:
            await dead_letter(message, attempt, exc)
        else:
            await schedule_retry(message, attempt, exc)


async def schedule_retry(message: RabbitMessage, attempt: int, error: Exception) -> None:
    logger.warning("retry %s, attempt %d", message.message_id, attempt)
    await broker.publish(
        message.body,
        queue=retry_queue(attempt),
        headers={**message.headers, ATTEMPT_HEADER: attempt + 1},
        message_id=message.message_id,
        content_type=message.content_type,
        persist=True,
    )


async def dead_letter(message: RabbitMessage, attempt: int, error: Exception) -> None:
    logger.error("to dlq %s", message.message_id)
    await broker.publish(
        message.body,
        exchange=dead_letter_exchange,
        routing_key=dead_letter_queue.name,
        headers={**message.headers, ATTEMPT_HEADER: attempt, "x-error": type(error).__name__},
        message_id=message.message_id,
        content_type=message.content_type,
        persist=True,
    )
