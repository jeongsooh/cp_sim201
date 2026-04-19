import paramiko

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

script = """
import serial
import time

port = '/dev/ttySTM5'
baud = 600

try:
    with serial.Serial(port, baud, timeout=0.2) as s:
        # Software Reset (if needed): s.write(b'\\x5E\\xFF\\xFF\\xFF\\xFE')
        # Start Conversion Command (Continuous) = 0xD5
        s.write(b'\\xD5')
        time.sleep(0.1)

        def cs_read_reg(page, reg):
            s.write(bytes([0x80 | page])) # Select Page
            s.write(bytes([0x20 | reg]))  # Read Command
            time.sleep(0.02)
            resp = s.read(3)
            if len(resp) == 3:
                return int.from_bytes(resp, byteorder='big', signed=True), resp.hex()
            return 0, resp.hex()
        
        print("Starting 5 consecutive reads of Vrms, Irms, and Pavg...")
        for i in range(5):
            v, v_hex = cs_read_reg(0x10, 0x06)
            a, a_hex = cs_read_reg(0x10, 0x05)
            p, p_hex = cs_read_reg(0x10, 0x0E)
            print(f"[{i}] Vrms: {v:10d} (0x{v_hex}) | Irms: {a:10d} (0x{a_hex}) | Pavg: {p:10d} (0x{p_hex})")
            time.sleep(0.5)

except Exception as e:
    print(f"Error: {e}")
"""

try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=10)

    sftp = client.open_sftp()
    with sftp.file('/tmp/diag_cs5490.py', 'w') as f:
        f.write(script)
    sftp.close()

    stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S /home/{user}/cp_sim201/venv/bin/python3 /tmp/diag_cs5490.py")
    print("STDOUT:")
    print(stdout.read().decode().strip())
    print("STDERR:")
    print(stderr.read().decode().strip())
    client.close()
except Exception as e:
    print(f"Error: {e}")
