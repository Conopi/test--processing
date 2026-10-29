from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app import services
from app.db import get_session
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from app.security import require_api_key

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)

Session = Annotated[AsyncSession, Depends(get_session)]
IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: IdempotencyKey,
    session: Session,
    response: Response,
) -> PaymentAccepted:
    try:
        payment, created = await services.create_payment(session, data, idempotency_key)
    except services.IdempotencyConflict:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail="Idempotency-Key has already been used with a different request body",
        )

    if not created:
        response.headers["Idempotent-Replayed"] = "true"

    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(payment_id: UUID, session: Session) -> PaymentRead:
    payment = await services.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentRead.model_validate(payment)
