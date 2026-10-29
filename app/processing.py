import asyncio
import logging
import random
from datetime import UTC, datetime
from uuid import UUID

import httpx
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment, PaymentStatus
from app.schemas import PaymentRead

logger = logging.getLogger(__name__)


class PaymentNotFound(Exception):
    pass


class GatewayEmulator:
    def __init__(self, success_rate: float, min_delay: float, max_delay: float) -> None:
        self._success_rate = success_rate
        self._min_delay = min_delay
        self._max_delay = max_delay

    async def charge(self, payment: Payment) -> bool:
        await asyncio.sleep(random.uniform(self._min_delay, self._max_delay))
        return random.random() < self._success_rate


class WebhookNotifier:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def notify(self, payment: Payment) -> None:
        body = {
            "event": f"payment.{payment.status}",
            "data": PaymentRead.model_validate(payment).model_dump(mode="json"),
        }
        response = await self._client.post(
            payment.webhook_url,
            json=body,
            headers={"Idempotency-Key": str(payment.id)},
        )
        response.raise_for_status()


class PaymentProcessor:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        gateway: GatewayEmulator,
        notifier: WebhookNotifier,
    ) -> None:
        self._sessions = sessions
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: UUID) -> Payment:
        payment = await self._settle(payment_id)
        await self._notifier.notify(payment)
        return payment

    async def _settle(self, payment_id: UUID) -> Payment:
        async with self._sessions() as session:
            payment = await session.get(Payment, payment_id)

        if payment is None:
            raise PaymentNotFound(payment_id)
        if payment.status is not PaymentStatus.PENDING:
            return payment

        succeeded = await self._gateway.charge(payment)
        new_status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED

        async with self._sessions() as session, session.begin():
            settled = await session.scalar(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(status=new_status, processed_at=datetime.now(UTC))
                .returning(Payment)
            )
            if settled is None:
                return await session.get_one(Payment, payment_id)

        logger.info("payment %s %s", payment_id, new_status)
        return settled
