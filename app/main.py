import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

from fastapi import FastAPI

from app.api import router
from app.config import settings
from app.db import engine, session_factory
from app.messaging import create_broker
from app.outbox import OutboxRelay

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    broker = create_broker()
    relay = OutboxRelay(
        broker,
        session_factory,
        poll_interval=settings.outbox_poll_interval,
        batch_size=settings.outbox_batch_size,
    )
    task = asyncio.create_task(relay.run(), name="outbox-relay")

    yield

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await broker.stop()
    await engine.dispose()


app = FastAPI(title="Payments Processing", version="1.0.0", lifespan=lifespan)
app.include_router(router)


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}
