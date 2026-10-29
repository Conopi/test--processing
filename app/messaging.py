from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

PAYMENTS_NEW = "payments.new"
PAYMENTS_DLQ = f"{PAYMENTS_NEW}.dlq"
ATTEMPT_HEADER = "x-attempt"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

payments_queue = RabbitQueue(
    PAYMENTS_NEW,
    durable=True,
    routing_key=PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": PAYMENTS_DLQ,
    },
)

dead_letter_queue = RabbitQueue(PAYMENTS_DLQ, durable=True, routing_key=PAYMENTS_DLQ)


def retry_delay(attempt: int) -> float:
    return settings.retry_base_delay * 2 ** (attempt - 1)


def retry_queue(attempt: int) -> RabbitQueue:
    return RabbitQueue(
        f"{PAYMENTS_NEW}.retry.{attempt}",
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay(attempt) * 1000),
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
        },
    )


def create_broker() -> RabbitBroker:
    return RabbitBroker(settings.rabbitmq_url)


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(payments_exchange)
    dlx = await broker.declare_exchange(dead_letter_exchange)

    queue = await broker.declare_queue(payments_queue)
    await queue.bind(exchange, routing_key=PAYMENTS_NEW)

    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(dlx, routing_key=PAYMENTS_DLQ)

    for attempt in range(1, settings.max_attempts):
        await broker.declare_queue(retry_queue(attempt))
