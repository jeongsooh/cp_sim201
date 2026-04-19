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
        s.write(b'\\x5E')
        time.sleep(0.5)

        def cs_read_reg(page, reg):
            s.write(bytes([0x80 | page])) # Select Page
            s.write(bytes([0x20 | reg]))  # Read Command
            time.sleep(0.02)
            resp = s.read(3)
            return resp.hex() if len(resp) == 3 else f"timeout(len={len(resp)})"
        
        regs_p0 = {0: "Config0", 1: "Config1", 2: "Config2", 16: "SysCtrl", 3: "OWR"}
        print("--- Page 0 Registers after Reset ---")
        for reg, name in regs_p0.items():
            print(f"{name} (0,{reg}): {cs_read_reg(0, reg)}")
            
        # Try to force SysCtrl to enable everything? 
        # Writing to SysCtrl is 0x40 | 16 = 0x50, wait, write is b5=0.
        # But first let's just see default values safely.
        
except Exception as e:
    print(f"Error: {e}")
"""

try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=10)

    sftp = client.open_sftp()
    with sftp.file('/tmp/diag_cs5490_4.py', 'w') as f:
        f.write(script)
    sftp.close()

    stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S /home/{user}/cp_sim201/venv/bin/python3 /tmp/diag_cs5490_4.py")
    print("STDOUT:")
    print(stdout.read().decode().strip())
    print("STDERR:")
    print(stderr.read().decode().strip())
    client.close()
except Exception as e:
    print(f"Error: {e}")
