"""
G4 — Offline Message Queue
네트워크 단절 시 TransactionEvent 등 중요 메세지를 data/offline_queue.jsonl에
저장하고, 재연결 후 순차 재전송한다. [OCPP 2.0.1 Part 2 - 4.1.1]
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DATA_DIR = os.path.join(_PROJECT_ROOT, "data")
_QUEUE_FILE = os.path.join(_DATA_DIR, "offline_queue.jsonl")


class OfflineMessageQueue:
    """파일 기반 OCPP 오프라인 메세지 큐 (JSONL 포맷)."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        # TC_E_29_CS: entries in-flight during drain — read from the file
        # but not yet ACKed by the CSMS. peek() must still see them so
        # GetTransactionStatus reports messagesInQueue=true while replay
        # is still underway.
        self._in_flight: List[Dict[str, Any]] = []
        os.makedirs(_DATA_DIR, exist_ok=True)

    async def enqueue(self, action: str, payload: Dict[str, Any]) -> None:
        """오프라인 시 발신 실패한 메세지를 큐 파일에 추가한다."""
        entry = {
            "action": action,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        async with self._lock:
            with open(_QUEUE_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        logger.warning(f"[OfflineQueue] Enqueued: {action} (queue={_QUEUE_FILE})")

    async def drain(self) -> List[Dict[str, Any]]:
        """큐 파일에 저장된 모든 메세지를 읽고 파일을 삭제한다.

        The returned entries are also held in self._in_flight so peek() keeps
        reporting them until ack_in_flight() is called (TC_E_29_CS).
        """
        async with self._lock:
            if not os.path.exists(_QUEUE_FILE):
                return []
            entries: List[Dict[str, Any]] = []
            with open(_QUEUE_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entries.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning(f"[OfflineQueue] Skipping malformed entry: {e}")
            os.remove(_QUEUE_FILE)
            self._in_flight = list(entries)
        logger.info(f"[OfflineQueue] Drained {len(entries)} messages")
        return entries

    async def ack_in_flight(self, entry: Dict[str, Any]) -> None:
        """Drop `entry` from the in-flight shadow once the CSMS ACKed it.

        Matching is by (action, payload, timestamp) identity — the same
        dict the caller received from drain().
        """
        async with self._lock:
            try:
                self._in_flight.remove(entry)
            except ValueError:
                pass

    async def clear_in_flight(self) -> None:
        """Clear the in-flight buffer after the drain loop terminates."""
        async with self._lock:
            self._in_flight = []

    def is_empty(self) -> bool:
        """큐가 비어 있으면 True를 반환한다."""
        if self._in_flight:
            return False
        return not os.path.exists(_QUEUE_FILE) or os.path.getsize(_QUEUE_FILE) == 0

    async def peek(self) -> List[Dict[str, Any]]:
        """큐에 저장된 메세지를 삭제하지 않고 반환한다. [TC_E_29_CS / E_31_CS / E_33_CS]

        Returns file-backed entries + in-flight entries (mid-drain) so
        GetTransactionStatus reports messagesInQueue=true until every
        replay has actually been ACKed.
        """
        async with self._lock:
            entries: List[Dict[str, Any]] = list(self._in_flight)
            if os.path.exists(_QUEUE_FILE):
                with open(_QUEUE_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entries.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            return entries
