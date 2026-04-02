import asyncio
import json
import pytest
import websockets
from websockets.server import serve

from src.config import OCPPConfig, RPCErrorCodes
from src.ocpp_client import OCPPClient, WaitQueueError

# Test Constants
TEST_STATION_ID = "TEST_STATION_001"
TEST_HOST = "localhost"
TEST_PORT = 9876
TEST_URL = f"ws://{TEST_HOST}:{TEST_PORT}/"

class DummyCSMSServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server = None
        self.recv_queue = asyncio.Queue()

    async def handle_client(self, websocket):
        try:
            async for message in websocket:
                msg_list = json.loads(message)
                await self.recv_queue.put(msg_list)
                
                # Auto-reply with CALLRESULT immediately for testing
                if msg_list[0] == OCPPConfig.MESSAGE_TYPE_CALL:
                    msg_id = msg_list[1]
                    action = msg_list[2]
                    
                    if action == "BootNotification":
                        response = [
                            OCPPConfig.MESSAGE_TYPE_RESULT,
                            msg_id,
                            {
                                "currentTime": "2026-04-02T12:00:00Z",
                                "interval": 300,
                                "status": "Accepted"
                            }
                        ]
                        await websocket.send(json.dumps(response))
                    elif action == "ErrorTest":
                        response = [
                            OCPPConfig.MESSAGE_TYPE_ERROR,
                            msg_id,
                            RPCErrorCodes.PROTOCOL_ERROR,
                            "Simulated error",
                            {}
                        ]
                        await websocket.send(json.dumps(response))
        except websockets.exceptions.ConnectionClosed:
            pass

    async def start(self):
        self.server = await serve(
            self.handle_client, 
            self.host, 
            self.port,
            subprotocols=[OCPPConfig.WEBSOCKET_SUBPROTOCOL]
        )

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()

@pytest.fixture
async def dummy_server():
    server = DummyCSMSServer(TEST_HOST, TEST_PORT)
    await server.start()
    yield server
    await server.stop()

@pytest.fixture
async def client():
    # Let the client connect
    client = OCPPClient(TEST_STATION_ID, TEST_URL)
    connect_task = asyncio.create_task(client.connect())
    
    # Wait for connection to establish
    await asyncio.sleep(0.5) 
    
    yield client
    
    await client.disconnect()
    connect_task.cancel()

@pytest.mark.asyncio
async def test_successful_call(dummy_server, client):
    """Test standard CALL and synchronous Wait Queue for CALLRESULT."""
    payload = {
        "reason": "PowerUp",
        "chargingStation": {
            "model": "AC_MODEL",
            "vendorName": "TEST_VENDOR"
        }
    }
    
    # Synchronously call and get result
    response = await client.call("BootNotification", payload, timeout=2.0)
    
    assert response["status"] == "Accepted"
    
    # Verify server received the CALL
    recv_msg = await dummy_server.recv_queue.get()
    assert recv_msg[0] == OCPPConfig.MESSAGE_TYPE_CALL
    assert recv_msg[2] == "BootNotification"

@pytest.mark.asyncio
async def test_error_call(dummy_server, client):
    """Test CALL receiving a CALLERROR."""
    with pytest.raises(WaitQueueError) as exc_info:
        await client.call("ErrorTest", {}, timeout=2.0)
    
    assert "ProtocolError" in str(exc_info.value)
    assert "Simulated error" in str(exc_info.value)
