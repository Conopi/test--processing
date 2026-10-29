import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import declare_topology, payments_exchange
from app.models import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        broker: RabbitBroker,
        sessions: async_sessionmaker[AsyncSession],
        *,
        poll_interval: float,
        batch_size: int,
    ) -> None:
        self._broker = broker
        self._sessions = sessions
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._ready = False

    async def run(self) -> None:
        while True:
            try:
                published = await self.publish_pending()
            except Exception:
                logger.error("outbox relay error")
                self._ready = False
                published = 0

            if published < self._batch_size:
                await asyncio.sleep(self._poll_interval)

    async def publish_pending(self) -> int:
        if not self._ready:
            await self._broker.connect()
            await declare_topology(self._broker)
            self._ready = True

        async with self._sessions() as session, session.begin():
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()

            published = 0
            for event in events:
                try:
                    await self._broker.publish(
                        event.payload,
                        exchange=payments_exchange,
                        routing_key=event.routing_key,
                        message_id=str(event.id),
                        persist=True,
                    )
                except Exception:
                    logger.error("publish failed %s", event.id)
                    self._ready = False
                    break
                event.published_at = datetime.now(UTC)
                published += 1

        if published:
            logger.info("published %d", published)
        return published
