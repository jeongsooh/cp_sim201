import pytest
import asyncio
from unittest.mock import AsyncMock, patch
import json

from src.ocpp_client import OCPPClient, WaitQueueError
from src.controller import ChargingStationController

TEST_STATION_ID = "TEST_STATION_001"
TEST_URL = "ws://localhost:9876"

@pytest.fixture
def ocpp_client():
    client = OCPPClient(TEST_STATION_ID, TEST_URL)
    # Mock ws to avoid real network connection
    client.ws = AsyncMock()
    return client

@pytest.mark.asyncio
async def test_invalid_payload_validation(ocpp_client):
    """Test that schema validation blocks outgoing invalid payload"""
    # BootNotificationRequest payload requires 'reason' and 'chargingStation'.
    # If we pass an empty dict, it should raise a validation error.
    with pytest.raises(WaitQueueError) as exc:
        await ocpp_client.call("BootNotification", {})
    
    assert "Validation Failed" in str(exc.value)

@pytest.mark.asyncio
async def test_server_call_handling(ocpp_client):
    """Test that incoming server CALLs (Reset) are routed to handlers"""
    # Init controller, which registers the "Reset" handler
    controller = ChargingStationController(ocpp_client)
    
    # Simulate an incoming ResetRequest CALL from CSMS
    message = '[2, "msg-1234", "Reset", {"type": "Immediate"}]'
    
    # Patch _send_result to capture the outgoing CALLRESULT
    with patch.object(ocpp_client, '_send_result', new=AsyncMock()) as mock_send_result:
        await ocpp_client._handle_message(message)
        
        # The registered handler should be called and send_result should deliver {"status": "Accepted"}
        mock_send_result.assert_called_once()
        args, kwargs = mock_send_result.call_args
        assert args[0] == "msg-1234"  # msg_id
        assert args[1]["status"] == "Accepted"

@pytest.mark.asyncio
async def test_heartbeat_task_starts_after_boot():
    # Use totally mocked client since we just test Controller's task logic
    client = AsyncMock(spec=OCPPClient)
    client.call.return_value = {"status": "Accepted", "interval": 1}
    
    controller = ChargingStationController(client)
    
    with patch('src.hal.HardwareAPI.check_proximity', return_value=False):
        await controller.boot_routine()
        
    assert controller._heartbeat_task is not None
    assert not controller._heartbeat_task.done()
    
    # Let it run briefly to verify it sends a heartbeat
    await asyncio.sleep(1.2)
    
    heartbeat_calls = [c for c in client.call.call_args_list if c[0][0] == "Heartbeat"]
    assert len(heartbeat_calls) > 0
    
    # Cleanup task
    controller._heartbeat_task.cancel()
