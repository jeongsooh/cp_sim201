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

logger = logging.getLogger(__name__)

class WaitQueueError(Exception):
    pass

class OCPPClient:
    def __init__(self, station_id: str, server_url: str):
        self.station_id = station_id
        self.server_url = server_url if server_url.endswith('/') else server_url + '/'
        self.uri = f"{self.server_url}{self.station_id}"
        
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._pending_calls: Dict[str, asyncio.Future] = {}
        self._pending_actions: Dict[str, str] = {} 
        self._listen_task: Optional[asyncio.Task] = None
        self._is_running = False
        
        self._action_handlers: Dict[str, Callable[[Dict], Awaitable[Dict]]] = {}
        self._schemas = self._load_schemas()

    def _load_schemas(self) -> Dict[str, dict]:
        schema_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schemas")
        schemas = {}
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

    def _validate_payload(self, schema_name: str, payload: dict):
        if schema_name in self._schemas:
            try:
                validate(instance=payload, schema=self._schemas[schema_name])
            except ValidationError as e:
                logger.error(f"Schema validation failed for {schema_name}: {e.message}")
                raise ValueError(f"Payload validation error: {e.message}")
        else:
            logger.warning(f"No schema found for {schema_name}, skipping validation.")

    def register_action_handler(self, action: str, handler: Callable[[Dict], Awaitable[Dict]]):
        self._action_handlers[action] = handler

    async def connect(self):
        self._is_running = True
        attempt = 0
        while self._is_running:
            try:
                logger.info(f"Attempting connection to {self.uri}")
                self.ws = await websockets.connect(
                    self.uri,
                    subprotocols=[OCPPConfig.WEBSOCKET_SUBPROTOCOL]
                )
                logger.info("Connection established.")
                attempt = 0
                self._listen_task = asyncio.create_task(self._listen())
                await self._listen_task
            except (ConnectionClosed, ConnectionRefusedError, OSError) as e:
                logger.warning(f"Connection error: {e}")
                self._cleanup_pending_calls()
                if not self._is_running:
                    break
                step = min(attempt, OCPPConfig.RETRY_BACKOFF_REPEAT_TIMES)
                wait_time = (OCPPConfig.RETRY_BACKOFF_WAIT_MINIMUM * (2 ** step)) + random.randint(0, OCPPConfig.RETRY_BACKOFF_RANDOM_RANGE)
                logger.info(f"Reconnecting in {wait_time} seconds (attempt {attempt+1})...")
                await asyncio.sleep(wait_time)
                attempt += 1

    async def _listen(self):
        try:
            async for message in self.ws:
                await self._handle_message(message)
        except ConnectionClosed:
            logger.warning("Listen task stopped due to ConnectionClosed")
            raise

    async def _handle_message(self, message: str):
        try:
            msg_list = json.loads(message)
            msg_type = msg_list[0]
            msg_id = msg_list[1]

            if msg_type == OCPPConfig.MESSAGE_TYPE_RESULT:
                payload = msg_list[2]
                action = self._pending_actions.get(msg_id)
                if action:
                    try:
                        self._validate_payload(f"{action}Response.json", payload)
                        self._resolve_pending_call(msg_id, payload)
                    except ValueError as ve:
                        self._resolve_pending_call_error(msg_id, RPCErrorCodes.FORMAT_VIOLATION, str(ve), {})
                else:
                    logger.warning(f"Received RESULT for unknown MsgId: {msg_id}")

            elif msg_type == OCPPConfig.MESSAGE_TYPE_ERROR:
                err_code = msg_list[2]
                err_desc = msg_list[3]
                err_details = msg_list[4] if len(msg_list) > 4 else {}
                self._resolve_pending_call_error(msg_id, err_code, err_desc, err_details)

            elif msg_type == OCPPConfig.MESSAGE_TYPE_CALL:
                action = msg_list[2]
                payload = msg_list[3]
                try:
                    self._validate_payload(f"{action}Request.json", payload)
                except ValueError as ve:
                    await self._send_error(msg_id, RPCErrorCodes.FORMAT_VIOLATION, str(ve))
                    return

                if action in self._action_handlers:
                    try:
                        resp_payload = await self._action_handlers[action](payload)
                        # Validate our own response before sending
                        self._validate_payload(f"{action}Response.json", resp_payload)
                        await self._send_result(msg_id, resp_payload)
                    except ValueError as ve:
                        await self._send_error(msg_id, RPCErrorCodes.INTERNAL_ERROR, f"Generated Response is invalid: {ve}")
                    except Exception as e:
                        await self._send_error(msg_id, RPCErrorCodes.INTERNAL_ERROR, str(e))
                else:
                    await self._send_error(msg_id, RPCErrorCodes.NOT_SUPPORTED, f"Action '{action}' is not supported yet")

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON message", exc_info=True)

    def _resolve_pending_call(self, msg_id: str, payload: Dict):
        if msg_id in self._pending_calls and not self._pending_calls[msg_id].done():
            self._pending_calls[msg_id].set_result(payload)

    def _resolve_pending_call_error(self, msg_id: str, err_code: str, err_desc: str, err_details: Dict):
        if msg_id in self._pending_calls and not self._pending_calls[msg_id].done():
            self._pending_calls[msg_id].set_exception(WaitQueueError(f"[{err_code}] {err_desc}"))

    def _cleanup_pending_calls(self):
        for msg_id, future in self._pending_calls.items():
            if not future.done():
                future.set_exception(ConnectionError("Connection lost."))
        self._pending_calls.clear()
        self._pending_actions.clear()

    async def disconnect(self):
        self._is_running = False
        if self.ws:
            await self.ws.close()
        if self._listen_task:
            self._listen_task.cancel()

    async def call(self, action: str, payload: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
        if not self.ws:
            raise ConnectionError("Not connected to CSMS")
            
        try:
            self._validate_payload(f"{action}Request.json", payload)
        except ValueError as e:
            raise WaitQueueError(f"Client Payload Validation Failed: {e}")

        msg_id = str(uuid.uuid4())
        call_msg = [OCPPConfig.MESSAGE_TYPE_CALL, msg_id, action, payload]
        
        future = asyncio.get_event_loop().create_future()
        self._pending_calls[msg_id] = future
        self._pending_actions[msg_id] = action

        try:
            await self.ws.send(json.dumps(call_msg))
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            raise WaitQueueError("Timeout waiting for response")
        finally:
            self._pending_calls.pop(msg_id, None)
            self._pending_actions.pop(msg_id, None)

    async def _send_result(self, msg_id: str, payload: Dict):
        if not self.ws:
            return
        await self.ws.send(json.dumps([OCPPConfig.MESSAGE_TYPE_RESULT, msg_id, payload]))

    async def _send_error(self, msg_id: str, error_code: str, error_description: str, error_details: Dict = None):
        if not self.ws:
            return
        await self.ws.send(json.dumps([OCPPConfig.MESSAGE_TYPE_ERROR, msg_id, error_code, error_description, error_details or {}]))
