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
        # Software Reset
        s.write(b'\\x5E')
        time.sleep(0.5)

        # Start Continuous Conversions
        s.write(b'\\xD5')
        time.sleep(0.2)

        def cs_read_reg(page, reg):
            s.write(bytes([0x80 | page])) # Select Page
            s.write(bytes([0x20 | reg]))  # Read Command
            time.sleep(0.02)
            resp = s.read(3)
            return resp.hex() if len(resp) == 3 else f"timeout(len={len(resp)})"
        
        for i in range(10):
            status = cs_read_reg(0, 23)   # Status0
            sysctrl = cs_read_reg(0, 16)  # Config0? No, Config0 is Reg 0. Wait, Config1 is Reg 1.
            vrms = cs_read_reg(16, 6)
            irms = cs_read_reg(16, 5)
            pavg = cs_read_reg(16, 14)
            print(f"[{i}] Status0(P0,R23): {status} | Vrms: {vrms} | Irms: {irms} | Pavg: {pavg}")
            time.sleep(0.5)

except Exception as e:
    print(f"Error: {e}")
"""

try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=10)

    sftp = client.open_sftp()
    with sftp.file('/tmp/diag_cs5490_3.py', 'w') as f:
        f.write(script)
    sftp.close()

    stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S /home/{user}/cp_sim201/venv/bin/python3 /tmp/diag_cs5490_3.py")
    print("STDOUT:")
    print(stdout.read().decode().strip())
    print("STDERR:")
    print(stderr.read().decode().strip())
    client.close()
except Exception as e:
    print(f"Error: {e}")
