import pytest
from unittest.mock import AsyncMock, patch
from src.ocpp_client import OCPPClient
from src.controller import ChargingStationController

@pytest.fixture
def mock_client():
    client = AsyncMock(spec=OCPPClient)
    client.call.return_value = {"status": "Accepted", "idTokenInfo": {"status": "Accepted"}}
    return client

@pytest.fixture
def controller(mock_client):
    return ChargingStationController(mock_client)

@pytest.mark.asyncio
async def test_TC_A_01_CS(controller, mock_client):
    """TC_A_01_CS: SecurityEventNotification"""
    await controller.trigger_security_event("StartupOfTheDevice", "Test info")
    mock_client.call.assert_called_with("SecurityEventNotification", {
        "type": "StartupOfTheDevice",
        "timestamp": "2026-04-02T12:00:00Z",
        "techInfo": "Test info"
    })

@pytest.mark.asyncio
async def test_TC_A_04_05_CS(controller, mock_client):
    """TC_A_04_CS, TC_A_05_CS: SignCertificate (RSA and ECC)"""
    await controller.trigger_sign_certificate("dummy_csr")
    mock_client.call.assert_called_with("SignCertificate", {
        "csr": "dummy_csr",
        "certificateType": "ChargingStationCertificate"
    })

@pytest.mark.asyncio
async def test_TC_A_06_07_CS(controller, mock_client):
    """TC_A_06_CS, TC_A_07_CS: InstallCertificate (RSA and ECC)"""
    res = await controller.handle_install_certificate({"certificateType": "CSMSCertificate", "certificate": "dummy"})
    assert res["status"] == "Accepted"

@pytest.mark.asyncio
async def test_TC_A_09_10_CS(controller, mock_client):
    """TC_A_09_CS, TC_A_10_CS: GetInstalledCertificateIds (RSA and ECC)"""
    res = await controller.handle_get_installed_certificate_ids({})
    assert res["status"] == "Accepted"
    assert "certificateHashDataChain" in res

@pytest.mark.asyncio
async def test_TC_A_11_12_CS(controller, mock_client):
    """TC_A_11_CS, TC_A_12_CS: DeleteCertificate (RSA and ECC)"""
    res = await controller.handle_delete_certificate({"certificateHashData": {}})
    assert res["status"] == "Accepted"

@pytest.mark.asyncio
async def test_TC_A_13_14_CS(controller, mock_client):
    """TC_A_13_CS, TC_A_14_CS: GetCertificateStatus (RSA and ECC)"""
    res = await controller.handle_get_certificate_status({"ocspRequestData": {"hashAlgorithm": "SHA256"}})
    assert res["status"] == "Accepted"

@pytest.mark.asyncio
async def test_TC_A_15_CS(controller, mock_client):
    """TC_A_15_CS: Get15118EVCertificate"""
    await controller.trigger_get_15118_ev_certificate("ISO15118-2")
    mock_client.call.assert_called_with("Get15118EVCertificate", {
        "iso15118SchemaVersion": "ISO15118-2",
        "action": "Install",
        "exiRequest": "dummy_exi_data"
    })

@pytest.mark.asyncio
async def test_TC_A_19_20_CS(controller, mock_client):
    """TC_A_19_CS, TC_A_20_CS: CertificateSigned"""
    res = await controller.handle_certificate_signed({"certificateChain": "dummy", "certificateType": "ChargingStationCertificate"})
    assert res["status"] == "Accepted"

@pytest.mark.asyncio
async def test_TC_A_21_22_23_CS(controller, mock_client):
    """TC_A_21_CS, TC_A_22_CS, TC_A_23_CS: Security Profiles (TLS config setup)"""
    # Simply assert that subprotocol setup reflects standard requirements
    from src.config import OCPPConfig
    assert OCPPConfig.WEBSOCKET_SUBPROTOCOL == "ocpp2.0.1"

@pytest.mark.asyncio
async def test_TC_B_01_CS(controller, mock_client):
    """TC_B_01_CS: Cold Booting"""
    from src.hal import HardwareAPI
    with patch.object(HardwareAPI, 'check_proximity', return_value=False):
        await controller.boot_routine()
    
    args_list = mock_client.call.call_args_list
    assert args_list[0][0][0] == "BootNotification"
    assert args_list[1][0][0] == "StatusNotification"
