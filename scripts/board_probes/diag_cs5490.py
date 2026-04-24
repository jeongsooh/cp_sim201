"""CS5490 UART 통신 진단 스크립트 — 보드에서 직접 실행"""
import serial
import time

PORT   = "/dev/ttySTM5"
BAUD   = 600

print("=" * 50)
print("CS5490 UART Diagnostic")
print(f"Port={PORT}  Baud={BAUD}")
print("=" * 50)

try:
    with serial.Serial(PORT, BAUD, timeout=1.0) as s:

        # 1. 초기화 시퀀스
        print("\n[1] Sync + Software Reset + Start Continuous Conversions")
        s.write(b'\xFF\xFF\xFF\xFE')
        time.sleep(0.2)
        s.write(b'\x5E')
        time.sleep(1.0)
        s.write(b'\xD5')
        time.sleep(0.5)
        print("    OK")

        # 2. 레지스터 읽기 함수 (byte간 충분한 딜레이 확보)
        def cs_read_reg(page, reg, label=""):
            s.reset_input_buffer()
            s.write(bytes([0x80 | page]))     # Page select
            time.sleep(0.06)                  # 1 byte @ 600baud = ~17ms, 여유분 포함
            s.write(bytes([0x20 | reg]))      # Read command
            time.sleep(0.06)
            resp = s.read(3)                  # 3 bytes @ 600baud = ~50ms, timeout=1.0s
            tag  = f"[{label}] " if label else ""
            print(f"    {tag}page=0x{page:02X} reg=0x{reg:02X}  "
                  f"resp({len(resp)}B)={resp.hex() if resp else '(empty)'}  ",
                  end="")
            if len(resp) == 3:
                val = int.from_bytes(resp, byteorder='big', signed=True)
                print(f"-> {val}")
                return val
            else:
                print("-> NO RESPONSE (timeout?)")
                return None

        print("\n[2] Register read (Page 16, timeout=1.0s)")
        v_raw = cs_read_reg(0x10, 0x06, "VRMS")
        i_raw = cs_read_reg(0x10, 0x05, "IRMS")
        p_raw = cs_read_reg(0x10, 0x0E, "POWER")

        print("\n[3] 2초 대기 후 재시도 (conversion 안정화)")
        time.sleep(2.0)
        v2 = cs_read_reg(0x10, 0x06, "VRMS_2")
        i2 = cs_read_reg(0x10, 0x05, "IRMS_2")
        p2 = cs_read_reg(0x10, 0x0E, "POWER_2")

        print("\n[4] Status Register (Page 0, Reg 0x17)")
        cs_read_reg(0x00, 0x17, "STATUS0")

        print("\n" + "=" * 50)
        print(f"SUMMARY:")
        print(f"  1st read: V={v_raw} I={i_raw} P={p_raw}")
        print(f"  2nd read: V={v2}    I={i2}    P={p2}")
        print("=" * 50)

except Exception as e:
    print(f"\nERROR: {e}")
