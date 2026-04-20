"""CS5490 Page 16 register scan — corrected for CS5490 datasheet command encoding.

CS5490 UART command format (Table 2):
  Register Read:  bits[7:6]=00 → addr & 0x3F   (NOT 0x20|addr!)
  Register Write: bits[7:6]=01 → 0x40 | addr
  Page Select:    bits[7:6]=10 → 0x80 | page
  Instruction:    bits[7:6]=11 → 0xC0 | code

Instructions (Table 3):
  0xC1 = Software Reset (NOT 0x5E — that was a register write!)
  0xD5 = Start Continuous Conversions
  WARNING: 0xFF = Gain Calibration instruction. Do NOT use as sync.

Page 16 register map (Table 6.3):
  0x02=I_inst, 0x03=V_inst, 0x04=P_inst
  0x05=PAVG, 0x06=IRMS, 0x07=VRMS
  0x0E=QAVG, 0x16=S, 0x17=PF, 0x1C=T
  0x24=IGAIN (default 0x400000), 0x26=VGAIN (default 0x400000)
  0x32=Epsilon, 0x33=SampleCount
"""
import serial
import time

PORT = "/dev/ttySTM5"
BAUD = 600

REG_NAMES = {
    0x00: "Config2",
    0x01: "RegChk",
    0x02: "I (instantaneous)",
    0x03: "V (instantaneous)",
    0x04: "P (instantaneous)",
    0x05: "PAVG (active power)",
    0x06: "IRMS",
    0x07: "VRMS",
    0x0E: "QAVG (reactive power)",
    0x0F: "Q (instantaneous)",
    0x16: "S (apparent power)",
    0x17: "PF (power factor)",
    0x1C: "T (temperature)",
    0x20: "PSUM",
    0x21: "SSUM",
    0x22: "QSUM",
    0x23: "IDCOFF",
    0x24: "IGAIN",
    0x25: "VDCOFF",
    0x26: "VGAIN",
    0x27: "POFF",
    0x28: "IACOFF",
    0x32: "Epsilon",
    0x33: "SampleCount",
}

def cs_read_reg(s, page, addr):
    s.reset_input_buffer()
    s.write(bytes([0x80 | page]))   # Page select
    time.sleep(0.06)
    s.write(bytes([addr & 0x3F]))   # Register Read (bits[7:6]=00)
    time.sleep(0.06)
    resp = s.read(3)
    if len(resp) == 3:
        raw = int.from_bytes(resp, byteorder='big', signed=False)
        signed = int.from_bytes(resp, byteorder='big', signed=True)
        return resp.hex(), raw, signed
    return None, None, None

print("=" * 70)
print("CS5490 Register Scan — Page 16 (corrected command encoding)")
print("=" * 70)

try:
    with serial.Serial(PORT, BAUD, timeout=1.0) as s:
        print("[Init] Wait for serial timeout to clear stale state")
        time.sleep(0.2)
        s.reset_input_buffer()

        print("[Init] Software Reset (0xC1)")
        s.write(b'\xC1')
        time.sleep(0.5)
        s.reset_input_buffer()

        print("[Init] Start Continuous Conversions (0xD5)")
        s.write(b'\xD5')
        time.sleep(1.5)
        s.reset_input_buffer()
        print("[Init] Done. First conversion cycle complete.")
        print()

        print(f"{'Addr':<6} {'Name':<30} {'Hex':<10} {'Unsigned':>12} {'Signed':>12}  {'*'}")
        print("-" * 78)

        scan_addrs = list(range(0x00, 0x08)) + list(range(0x0E, 0x10)) + \
                     [0x16, 0x17, 0x1C] + list(range(0x20, 0x29)) + [0x32, 0x33]

        for addr in scan_addrs:
            name = REG_NAMES.get(addr, "")
            hx, raw, signed = cs_read_reg(s, 0x10, addr)
            if hx is None:
                print(f"0x{addr:02X}   {name:<30} (no response)")
            else:
                marker = " <--" if raw != 0 else ""
                print(f"0x{addr:02X}   {name:<30} {hx:<10} {raw:>12} {signed:>12}{marker}")

        print()
        print("=" * 70)
        print("Page 0 — Config & Status Registers")
        print("=" * 70)
        p0 = {0x00: "Config0", 0x01: "Config1", 0x03: "Mask",
              0x05: "PC", 0x07: "SerialCtrl", 0x17: "Status0", 0x18: "Status1"}
        for addr, name in p0.items():
            hx, raw, signed = cs_read_reg(s, 0x00, addr)
            if hx:
                marker = " <--" if raw != 0 else ""
                print(f"0x{addr:02X}   {name:<30} {hx:<10} {raw:>12}{marker}")

except Exception as e:
    print(f"ERROR: {e}")
