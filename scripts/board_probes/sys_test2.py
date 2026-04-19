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
        # Start Conversion
        s.write(b'\\xD5')
        time.sleep(0.5)

        def cs_read_reg(page, reg):
            s.write(bytes([0x80 | page])) # Select Page
            s.write(bytes([0x20 | reg]))  # Read Command
            time.sleep(0.01)
            resp = s.read(3)
            return resp.hex() if len(resp) == 3 else "timeout"
        
        print("Dumping Page 16 (DSP Outputs)...")
        for reg in range(32):
            val = cs_read_reg(16, reg)
            if val != "timeout" and val != "000000":
                print(f"Reg {reg:2d} (0x{reg:02X}): {val}")

except Exception as e:
    print(f"Error: {e}")
"""

try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=10)

    sftp = client.open_sftp()
    with sftp.file('/tmp/diag_cs5490_all.py', 'w') as f:
        f.write(script)
    sftp.close()

    stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S /home/{user}/cp_sim201/venv/bin/python3 /tmp/diag_cs5490_all.py")
    print("STDOUT:")
    print(stdout.read().decode().strip())
    print("STDERR:")
    print(stderr.read().decode().strip())
    client.close()
except Exception as e:
    print(f"Error: {e}")
