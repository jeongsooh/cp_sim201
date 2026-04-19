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

        # Write to SysCtrl (Page 0, Reg 16) to power up V & I ADCs
        # Write Command: 0x10 (16), Data: 0x300000
        s.write(b'\\x80') # Select Page 0
        s.write(bytes([16])) # Write Cmd for Reg 16
        s.write(b'\\x30\\x00\\x00') # Turn on V P-Up and I P-Up (Bits 21 and 20)
        time.sleep(0.1)

        # Start Continuous Conversions
        s.write(b'\\xD5')
        time.sleep(0.2)

        def cs_read_reg(page, reg):
            s.write(bytes([0x80 | page])) # Select Page
            s.write(bytes([0x20 | reg]))  # Read Command
            time.sleep(0.02)
            resp = s.read(3)
            return resp.hex() if len(resp) == 3 else f"timeout({len(resp)})"
        
        for i in range(5):
            status = cs_read_reg(0, 23)
            sysctrl = cs_read_reg(0, 16)
            vrms = cs_read_reg(16, 6)
            irms = cs_read_reg(16, 5)
            pavg = cs_read_reg(16, 14)
            print(f"[{i}] SysCtrl: {sysctrl} | Status0: {status} | Vrms: {vrms} | Irms: {irms} | Pavg: {pavg}")
            time.sleep(0.5)

except Exception as e:
    print(f"Error: {e}")
"""

try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=10)

    sftp = client.open_sftp()
    with sftp.file('/tmp/diag_cs5490_5.py', 'w') as f:
        f.write(script)
    sftp.close()

    stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S /home/{user}/cp_sim201/venv/bin/python3 /tmp/diag_cs5490_5.py")
    print("STDOUT:")
    print(stdout.read().decode().strip())
    print("STDERR:")
    print(stderr.read().decode().strip())
    client.close()
except Exception as e:
    print(f"Error: {e}")
