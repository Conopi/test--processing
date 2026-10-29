import hashlib
import json
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging import PAYMENTS_NEW
from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    pass


def request_fingerprint(data: PaymentCreate) -> str:
    raw = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> tuple[Payment, bool]:
    fingerprint = request_fingerprint(data)

    stmt = (
        insert(Payment)
        .values(
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            meta=data.metadata,
            webhook_url=str(data.webhook_url),
            idempotency_key=idempotency_key,
            request_hash=fingerprint,
        )
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )
    payment = (await session.scalars(stmt)).one_or_none()

    if payment is None:
        await session.rollback()
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None or existing.request_hash != fingerprint:
            raise IdempotencyConflict(idempotency_key)
        return existing, False

    session.add(
        OutboxEvent(
            aggregate_id=payment.id,
            routing_key=PAYMENTS_NEW,
            payload={"payment_id": str(payment.id)},
        )
    )
    await session.commit()
    return payment, True


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
