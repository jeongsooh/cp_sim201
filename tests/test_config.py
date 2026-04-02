from src.config import OCPPConfig, ACChargingConfig

def test_config_constants():
    assert OCPPConfig.MESSAGE_TYPE_CALL == 2
    assert ACChargingConfig.SUPPLY_PHASES == 3
    assert "Voltage" in ACChargingConfig.MEASURANDS
