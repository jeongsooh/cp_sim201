import asyncio
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
    # Represent an online ws so stop_transaction takes the online branch
    # (TC_E_45_CS offline scan-stop split is gated on a live ws).
    client.ws = object()
    return client

@pytest.fixture
def controller(mock_client):
    return ChargingStationController(mock_client)

@pytest.mark.asyncio
async def test_boot_routine(controller, mock_client):
    with patch.object(HardwareAPI, 'check_proximity', return_value=False):
        await controller.boot_routine()
    
    # Should call BootNotification and then StatusNotification
    assert mock_client.call.call_count == 2
    args_list = mock_client.call.call_args_list
    assert args_list[0][0][0] == "BootNotification"
    assert args_list[1][0][0] == "StatusNotification"

@pytest.mark.asyncio
async def test_transaction_flow(controller, mock_client):
    # 1. User plugs in cable
    # TxStartPoint includes "EVConnected" — OCPP 2.0.1 §E02 OR semantics means
    # the transaction is started on cable plug, before any authorization.
    with patch.object(HardwareAPI, 'check_proximity', return_value=True):
        await controller.simulate_cable_plugged()

    assert controller.connector_hal.status == "Occupied"
    assert controller.is_authorized is False
    assert controller.transaction_id is not None
    tx_id = controller.transaction_id

    # StatusNotification + TransactionEvent(Started, triggerReason=EVConnected)
    args_list = mock_client.call.call_args_list
    assert args_list[0][0][0] == "StatusNotification"
    assert args_list[1][0][0] == "TransactionEvent"
    assert args_list[1][0][1]["eventType"] == "Started"
    assert args_list[1][0][1]["triggerReason"] == "CablePluggedIn"

    # Reset mock to clarify counts
    mock_client.call.reset_mock()

    # 2. User Scans RFID — authorize existing tx, energize relay, send Updated
    with patch.object(HardwareAPI, 'relay_on') as mock_relay_close:
        await controller.handle_rfid_scan("TEST_UID")

    assert mock_client.call.call_count == 2
    args_list = mock_client.call.call_args_list
    assert args_list[0][0][0] == "Authorize"
    assert args_list[1][0][0] == "TransactionEvent"
    assert args_list[1][0][1]["eventType"] == "Updated"
    assert args_list[1][0][1]["triggerReason"] == "Authorized"

    assert controller.is_authorized is True
    assert controller.transaction_id == tx_id
    mock_relay_close.assert_called_once_with(1)
    
    # Reset mock
    mock_client.call.reset_mock()
    
    # 3. User Scans RFID again to Stop
    with patch.object(HardwareAPI, 'relay_off') as mock_relay_open:
        await controller.handle_rfid_scan("TEST_UID")

    # Per OCPP 2.0.1 §C03, a stop-scan of an already-authorized active tx
    # must NOT re-issue AuthorizeRequest — it goes straight to
    # TransactionEvent(Ended).
    assert mock_client.call.call_count == 1
    args_list = mock_client.call.call_args_list
    assert args_list[0][0][0] == "TransactionEvent"
    assert args_list[0][0][1]["eventType"] == "Ended"
    assert args_list[0][0][1]["transactionInfo"]["stoppedReason"] == "Local"

    assert controller.is_authorized is False
    assert controller.transaction_id is None
    mock_relay_open.assert_called_once_with(1)
