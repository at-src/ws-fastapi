from fastapi import WebSocket
from collections import defaultdict
from typing import Dict, Set
import logging

logger = logging.getLogger(__name__)


class RoomManager:
    """Manages WebSocket connections grouped by room_id."""

    def __init__(self):
        self._rooms: Dict[str, Set[WebSocket]] = defaultdict(set)

    async def connect(self, ws: WebSocket, room_id: str):
        await ws.accept()
        self._rooms[room_id].add(ws)
        logger.debug("connect room=%s total=%d", room_id, len(self._rooms[room_id]))

    def disconnect(self, ws: WebSocket, room_id: str):
        self._rooms[room_id].discard(ws)
        if not self._rooms[room_id]:
            del self._rooms[room_id]

    async def broadcast(self, room_id: str, payload: dict):
        """Send payload to all clients in room. Dead connections are pruned."""
        dead = set()
        for ws in self._rooms.get(room_id, set()):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)

        for ws in dead:
            self._rooms[room_id].discard(ws)

    def room_count(self, room_id: str) -> int:
        return len(self._rooms.get(room_id, set()))

    def active_rooms(self) -> list[str]:
        return list(self._rooms.keys())


manager = RoomManager()
