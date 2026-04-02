import pytest
from unittest.mock import AsyncMock, patch
from src.hal import ConnectorHAL, TokenReaderHAL, PowerContactorHAL, HardwareAPI

@pytest.mark.asyncio
async def test_connector_status_change():
    mock_client = AsyncMock()
    connector = ConnectorHAL(evse_id=1, connector_id=1, ocpp_client=mock_client)
    
    # Simulate someone plugging in the cable
    with patch.object(HardwareAPI, 'check_proximity', return_value=True):
        await connector.on_status_change()
        
    assert connector.status == "Occupied"
    mock_client.call.assert_called_once()
    args, kwargs = mock_client.call.call_args
    assert args[0] == "StatusNotification"
    assert args[1]["connectorStatus"] == "Occupied"
    assert args[1]["evseId"] == 1
    assert args[1]["connectorId"] == 1

@pytest.mark.asyncio
async def test_connector_no_redundant_call():
    mock_client = AsyncMock()
    connector = ConnectorHAL(evse_id=1, connector_id=1, ocpp_client=mock_client)
    connector.status = "Occupied"
    
    # Assuming the plug stays in
    with patch.object(HardwareAPI, 'check_proximity', return_value=True):
        await connector.on_status_change()
        
    # State didn't change, so no call should be made
    mock_client.call.assert_not_called()

@pytest.mark.asyncio
async def test_token_reader_scan():
    mock_client = AsyncMock()
    reader = TokenReaderHAL(ocpp_client=mock_client)
    
    await reader.on_rfid_scanned("DEADBEEF")
    
    mock_client.call.assert_called_once()
    args, kwargs = mock_client.call.call_args
    assert args[0] == "Authorize"
    assert args[1]["idToken"]["idToken"] == "DEADBEEF"

@pytest.mark.asyncio
async def test_token_reader_disabled():
    mock_client = AsyncMock()
    reader = TokenReaderHAL(ocpp_client=mock_client)
    reader.set_enabled(False)
    
    await reader.on_rfid_scanned("DEADBEEF")
    
    # Should ignore the scan
    mock_client.call.assert_not_called()

def test_power_contactor():
    contactor = PowerContactorHAL(evse_id=1)
    
    with patch.object(HardwareAPI, 'relay_on') as mock_on:
        contactor.control_relay("Close")
        mock_on.assert_called_once_with(1)

    with patch.object(HardwareAPI, 'relay_off') as mock_off:
        contactor.control_relay("Open")
        mock_off.assert_called_once_with(1)

    with patch.object(HardwareAPI, 'get_relay_status', return_value=True):
        assert contactor.get_actual_active_state() is True
