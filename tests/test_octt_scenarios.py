import pytest
from unittest.mock import AsyncMock, patch
from src.ocpp_client import OCPPClient
from src.controller import ChargingStationController
from src.hal import HardwareAPI

@pytest.fixture
def mock_client():
    client = AsyncMock(spec=OCPPClient)
    # Give a default successful response for all calls
    client.call.return_value = {"status": "Accepted", "idTokenInfo": {"status": "Accepted"}}
    return client

@pytest.fixture
def controller(mock_client):
    return ChargingStationController(mock_client)


@pytest.mark.asyncio
async def test_TC_B_01_CS_cold_booting(controller, mock_client):
    """
    [OCTT] TC_B_01_CS: Cold Booting
    Verify that the Charging Station sends BootNotification upon startup,
    and reports its initial Status as 'Available'.
    """
    with patch.object(HardwareAPI, 'check_proximity', return_value=False):
        await controller.boot_routine()
    
    args_list = mock_client.call.call_args_list
    assert len(args_list) >= 2, "Expected at least 2 OCPP calls: Boot and Status"
    
    assert args_list[0][0][0] == "BootNotification", "First call must be BootNotification"
    assert args_list[1][0][0] == "StatusNotification", "Second call must be StatusNotification"
    assert args_list[1][0][1]["connectorStatus"] == "Available", "Initial status should be Available"


@pytest.mark.asyncio
async def test_TC_E_01_CS_start_transaction(controller, mock_client):
    """
    [OCTT] TC_E_01_CS: Start Transaction
    Verify PowerPathClosed and Authorization correctly trigger TransactionEvent(eventType=Started)
    and physical relay closes.
    """
    # 1. Authorize logic execution
    await controller.handle_rfid_scan("VALID_ID_OK")
    assert controller.is_authorized is True, "Controller must mark as Authorized"
    
    # 2. Cable Plugged In with PowerContactor Close Mock
    with patch.object(HardwareAPI, 'check_proximity', return_value=True):
        with patch.object(HardwareAPI, 'relay_on') as mock_relay:
            await controller.simulate_cable_plugged()
            
            # Relay should be closed to supply AC Power
            mock_relay.assert_called_once_with(1)
            
    # Verification of Transaction Started.
    # TxStartPoint "Authorized,EVConnected" (OR semantics) means the RFID scan
    # starts the transaction immediately with triggerReason=Authorized; the
    # later cable plug emits Updated(CablePluggedIn) and — because the cable
    # plug with an authorized tx transitions the charging state — also
    # Updated(ChargingStateChanged=Charging) (TC_E_16_CS auto-fire).
    args_list = mock_client.call.call_args_list
    tx_events = [c for c in args_list if c[0][0] == "TransactionEvent"]

    assert len(tx_events) == 3, "Expected Started + CablePluggedIn + ChargingStateChanged TransactionEvents"
    assert tx_events[0][0][1]["eventType"] == "Started"
    assert tx_events[0][0][1]["triggerReason"] == "Authorized"
    assert tx_events[1][0][1]["eventType"] == "Updated"
    assert tx_events[1][0][1]["triggerReason"] == "CablePluggedIn"
    assert tx_events[2][0][1]["eventType"] == "Updated"
    assert tx_events[2][0][1]["triggerReason"] == "ChargingStateChanged"
    assert tx_events[2][0][1]["transactionInfo"]["chargingState"] == "Charging"
    assert controller.transaction_id is not None, "Controller must persist an active Transaction ID"
