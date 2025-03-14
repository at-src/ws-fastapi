import asyncio
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import Any

from broker import manager
from pubsub import subscriber_loop, publish

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(subscriber_loop())
    yield
    task.cancel()
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
    except Exception:
        pass


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws/{room_id}")
async def ws_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(websocket, room_id)
    try:
        while True:
            # keep-alive; actual messages come via Redis pub/sub
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)


class PublishRequest(BaseModel):
    payload: dict[str, Any]


@app.post("/publish/{room_id}", status_code=202)
async def publish_to_room(room_id: str, body: PublishRequest):
    """Push a message to all WebSocket clients in room_id."""
    await publish(room_id, body.payload)
    return {"room_id": room_id, "clients": manager.room_count(room_id)}


@app.get("/rooms")
async def list_rooms():
    return {"rooms": manager.active_rooms()}


@app.get("/health")
async def health():
    return {"status": "ok"}
