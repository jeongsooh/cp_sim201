import asyncio
import json
import logging
import uuid
import random
import os
import websockets
from websockets.exceptions import ConnectionClosed
from typing import Dict, Any, Optional, Callable, Awaitable
from jsonschema import validate, ValidationError

from .config import OCPPConfig, RPCErrorCodes
from .message_queue import OfflineMessageQueue

logger = logging.getLogger(__name__)


class WaitQueueError(Exception):
    pass


class OCPPClient:
    def __init__(
        self,
        station_id: str,
        server_url: str,
        ws_kwargs: Optional[dict] = None,
    ) -> None:
        self.station_id = station_id
        self.server_url = server_url if server_url.endswith("/") else server_url + "/"
        self.uri = f"{self.server_url}{self.station_id}"
        self._ws_kwargs = ws_kwargs or {}

        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._pending_calls: Dict[str, asyncio.Future] = {}
        self._pending_actions: Dict[str, str] = {}
        self._listen_task: Optional[asyncio.Task] = None
        self._is_running = False

        self._action_handlers: Dict[str, Callable[[Dict], Awaitable[Dict]]] = {}
        self._schemas = self._load_schemas()
        self.offline_queue = OfflineMessageQueue()

    def _load_schemas(self) -> Dict[str, dict]:
        schema_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schemas")
        schemas: Dict[str, dict] = {}
        if not os.path.isdir(schema_dir):
            logger.warning(f"Schema directory not found: {schema_dir}")
            return schemas

        for file in os.listdir(schema_dir):
            if file.endswith(".json"):
                path = os.path.join(schema_dir, file)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        schemas[file] = json.load(f)
                except Exception as e:
                    logger.error(f"Failed to parse schema {file}: {e}")
        return schemas

    def _validate_payload(self, schema_name: str, payload: dict) -> None:
        if schema_name not in self._schemas:
            logger.warning(f"No schema found for {schema_name}, skipping validation.")
            return
        try:
            validate(instance=payload, schema=self._schemas[schema_name])
        except ValidationError as e:
            # TypeError/path 정보로 FormatViolation vs TypeConstraintViolation 구분
            if e.validator in ("type", "enum"):
                raise ValueError(
                    f"{RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION}: {e.message}"
                )
            raise ValueError(
                f"{RPCErrorCodes.FORMAT_VIOLATION}: {e.message}"
            )

    def register_action_handler(
        self, action: str, handler: Callable[[Dict], Awaitable[Dict]]
    ) -> None:
        self._action_handlers[action] = handler

    async def connect(self) -> None:
        self._is_running = True
        attempt = 0
        while self._is_running:
            try:
                logger.info(f"Attempting connection to {self.uri}")
                self.ws = await websockets.connect(
                    self.uri,
                    subprotocols=[OCPPConfig.WEBSOCKET_SUBPROTOCOL],
                    **self._ws_kwargs,
                )
                logger.info("Connection established.")
                attempt = 0
                # G4: 재연결 직후 오프라인 큐 재전송
                asyncio.create_task(self._drain_offline_queue())
                self._listen_task = asyncio.create_task(self._listen())
                await self._listen_task
            except (ConnectionClosed, ConnectionRefusedError, OSError) as e:
                logger.warning(f"Connection error: {e}")
                self._cleanup_pending_calls()
                if not self._is_running:
                    break
                step = min(attempt, OCPPConfig.RETRY_BACKOFF_REPEAT_TIMES)
                wait_time = (
                    OCPPConfig.RETRY_BACKOFF_WAIT_MINIMUM * (2 ** step)
                    + random.randint(0, OCPPConfig.RETRY_BACKOFF_RANDOM_RANGE)
                )
                logger.info(f"Reconnecting in {wait_time}s (attempt {attempt + 1})...")
                await asyncio.sleep(wait_time)
                attempt += 1

    async def _drain_offline_queue(self) -> None:
        """재연결 후 오프라인 큐에 저장된 메세지를 순차 재전송한다."""
        if self.offline_queue.is_empty():
            return
        entries = await self.offline_queue.drain()
        logger.info(f"[OfflineQueue] Replaying {len(entries)} queued messages")
        for entry in entries:
            try:
                await self.call(entry["action"], entry["payload"])
                logger.info(f"[OfflineQueue] Replayed: {entry['action']}")
            except Exception as e:
                logger.error(f"[OfflineQueue] Failed to replay {entry['action']}: {e}")

    async def _listen(self) -> None:
        try:
            async for message in self.ws:
                await self._handle_message(message)
        except ConnectionClosed:
            logger.warning("Listen task stopped due to ConnectionClosed")
            raise

    async def _handle_message(self, message: str) -> None:
        try:
            msg_list = json.loads(message)
        except json.JSONDecodeError as e:
            logger.error(f"[ProtocolError] Failed to decode JSON: {e}")
            return

        try:
            msg_type = msg_list[0]
            msg_id   = msg_list[1]
        except (IndexError, TypeError) as e:
            logger.error(f"[ProtocolError] Malformed message frame: {e}")
            return

        logger.info(f"WS RECV: msgId={msg_id} type={msg_type} raw={message[:200]}")

        if msg_type == OCPPConfig.MESSAGE_TYPE_RESULT:
            payload = msg_list[2]
            action  = self._pending_actions.get(msg_id)
            if action:
                try:
                    self._validate_payload(f"{action}Response.json", payload)
                    self._resolve_pending_call(msg_id, payload)
                except ValueError as ve:
                    error_code = (
                        RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION
                        if RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION in str(ve)
                        else RPCErrorCodes.FORMAT_VIOLATION
                    )
                    self._resolve_pending_call_error(msg_id, error_code, str(ve), {})
            else:
                logger.warning(f"Received RESULT for unknown msgId={msg_id}")

        elif msg_type == OCPPConfig.MESSAGE_TYPE_ERROR:
            err_code    = msg_list[2]
            err_desc    = msg_list[3]
            err_details = msg_list[4] if len(msg_list) > 4 else {}
            logger.warning(f"RPC Error received: msgId={msg_id} code={err_code} desc={err_desc}")
            self._resolve_pending_call_error(msg_id, err_code, err_desc, err_details)

        elif msg_type == OCPPConfig.MESSAGE_TYPE_CALL:
            action  = msg_list[2]
            payload = msg_list[3]
            logger.info(f"WS CALL: msgId={msg_id} action={action}")
            try:
                self._validate_payload(f"{action}Request.json", payload)
            except ValueError as ve:
                error_code = (
                    RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION
                    if RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION in str(ve)
                    else RPCErrorCodes.FORMAT_VIOLATION
                )
                await self._send_error(msg_id, error_code, str(ve))
                return

            if action in self._action_handlers:
                try:
                    resp_payload = await self._action_handlers[action](payload)
                    self._validate_payload(f"{action}Response.json", resp_payload)
                    await self._send_result(msg_id, resp_payload)
                except ValueError as ve:
                    await self._send_error(
                        msg_id, RPCErrorCodes.INTERNAL_ERROR,
                        f"Generated response is invalid: {ve}"
                    )
                except Exception as e:
                    logger.error(f"Handler exception for {action}: {e}", exc_info=True)
                    await self._send_error(msg_id, RPCErrorCodes.INTERNAL_ERROR, str(e))
            else:
                logger.warning(f"Action '{action}' has no registered handler")
                await self._send_error(
                    msg_id, RPCErrorCodes.NOT_SUPPORTED,
                    f"Action '{action}' is not supported"
                )
        else:
            logger.warning(f"[ProtocolError] Unknown message type: {msg_type}")

    def _resolve_pending_call(self, msg_id: str, payload: Dict) -> None:
        if msg_id in self._pending_calls and not self._pending_calls[msg_id].done():
            self._pending_calls[msg_id].set_result(payload)

    def _resolve_pending_call_error(
        self, msg_id: str, err_code: str, err_desc: str, err_details: Dict
    ) -> None:
        if msg_id in self._pending_calls and not self._pending_calls[msg_id].done():
            self._pending_calls[msg_id].set_exception(
                WaitQueueError(f"[{err_code}] {err_desc}")
            )

    def _cleanup_pending_calls(self) -> None:
        for future in self._pending_calls.values():
            if not future.done():
                future.set_exception(ConnectionError("Connection lost."))
        self._pending_calls.clear()
        self._pending_actions.clear()

    async def disconnect(self) -> None:
        self._is_running = False
        if self.ws:
            await self.ws.close()
        if self._listen_task:
            self._listen_task.cancel()

    async def call(
        self,
        action: str,
        payload: Dict[str, Any],
        timeout: float = 30.0,
        allow_offline: bool = False,
    ) -> Dict[str, Any]:
        """CSMS에 CALL 메세지를 전송하고 CALLRESULT를 반환한다.

        allow_offline=True 지정 시, 연결 불가 상태에서 메세지를 오프라인 큐에
        저장하고 빈 dict를 반환한다. [OCPP 2.0.1 Part 2 - 4.1.1]
        """
        if not self.ws:
            if allow_offline:
                await self.offline_queue.enqueue(action, payload)
                return {}
            raise ConnectionError("Not connected to CSMS")

        try:
            self._validate_payload(f"{action}Request.json", payload)
        except ValueError as e:
            raise WaitQueueError(f"Client payload validation failed: {e}")

        msg_id   = str(uuid.uuid4())
        call_msg = [OCPPConfig.MESSAGE_TYPE_CALL, msg_id, action, payload]

        future = asyncio.get_event_loop().create_future()
        self._pending_calls[msg_id]  = future
        self._pending_actions[msg_id] = action

        try:
            raw_msg = json.dumps(call_msg)
            logger.info(f"WS SEND: msgId={msg_id} action={action} payload={raw_msg[:200]}")
            await self.ws.send(raw_msg)
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            raise WaitQueueError(f"Timeout waiting for response to {action}")
        finally:
            self._pending_calls.pop(msg_id, None)
            self._pending_actions.pop(msg_id, None)

    async def _send_result(self, msg_id: str, payload: Dict) -> None:
        if not self.ws:
            return
        raw = json.dumps([OCPPConfig.MESSAGE_TYPE_RESULT, msg_id, payload])
        logger.info(f"WS SEND RESULT: msgId={msg_id}")
        await self.ws.send(raw)

    async def _send_error(
        self,
        msg_id: str,
        error_code: str,
        error_description: str,
        error_details: Optional[Dict] = None,
    ) -> None:
        if not self.ws:
            return
        raw = json.dumps([
            OCPPConfig.MESSAGE_TYPE_ERROR,
            msg_id,
            error_code,
            error_description,
            error_details or {},
        ])
        logger.warning(f"WS SEND ERROR: msgId={msg_id} code={error_code} desc={error_description}")
        await self.ws.send(raw)
