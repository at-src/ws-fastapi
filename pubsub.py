import asyncio
import json
import logging

import redis.asyncio as aioredis

from config import REDIS_HOST, REDIS_PORT, REDIS_DB, CHANNEL_PREFIX
from broker import manager

logger = logging.getLogger(__name__)

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = await aioredis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
            decode_responses=True,
        )
    return _redis


async def publish(room_id: str, payload: dict):
    """Publish a message to a room's Redis channel."""
    r = await get_redis()
    await r.publish(f"{CHANNEL_PREFIX}{room_id}", json.dumps(payload))


async def subscriber_loop():
    """
    Runs forever. Subscribes to all broker channels via pattern
    and forwards messages to the local RoomManager.
    """
    r = await aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
        decode_responses=True,
    )
    pubsub = r.pubsub()
    await pubsub.psubscribe(f"{CHANNEL_PREFIX}*")
    logger.info("subscriber_loop started, pattern=%s*", CHANNEL_PREFIX)

    async for msg in pubsub.listen():
        if msg["type"] != "pmessage":
            continue
        try:
            channel: str = msg["channel"]          # e.g. "wsb:room-42"
            room_id = channel[len(CHANNEL_PREFIX):]
            payload = json.loads(msg["data"])
            await manager.broadcast(room_id, payload)
        except Exception as e:
            logger.warning("subscriber_loop error: %s", e)
