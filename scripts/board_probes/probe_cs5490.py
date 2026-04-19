import paramiko

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

script = """
import serial
import time
import os

ports = ['/dev/ttySTM0', '/dev/ttySTM1', '/dev/ttySTM2', '/dev/ttySTM3', '/dev/ttySTM4', '/dev/ttySTM5', '/dev/ttySTM6']
bauds = [600, 4800, 9600, 38400, 115200]

found = False
for p in ports:
    if not os.path.exists(p): continue
    for b in bauds:
        try:
            # open port
            s = serial.Serial(p, b, timeout=0.1)
            # Sync sequence to wake CS5490 up or reset command port:
            s.write(b'\\xFF\\xFF\\xFF\\xFE')
            time.sleep(0.05)
            # Send 'Read Register 0 (Config)' command: 0x00
            s.write(b'\\x00')
            time.sleep(0.05)
            resp = s.read(10)
            if resp:
                print(f"[{p} @ {b} baud] RESP: {resp.hex()}")
                found = True
            s.close()
        except Exception as e:
            pass

if not found:
    print("Could not get a response from any port/baud combination.")
"""

try:
    print(f"Connecting to {host}...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=10)

    sftp = client.open_sftp()
    with sftp.file('/tmp/test_cs.py', 'w') as f:
        f.write(script)
    sftp.close()

    stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S /home/{user}/cp_sim201/venv/bin/python3 /tmp/test_cs.py")
    print("STDOUT:", stdout.read().decode().strip())
    print("STDERR:", stderr.read().decode().strip())
    client.close()
except Exception as e:
    print(f"Error: {e}")
