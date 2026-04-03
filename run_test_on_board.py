import paramiko
import time

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    print(f"Connecting to target board at {host}...")
    client.connect(hostname=host, username=user, password=pwd, timeout=10)
    
    print("\n[1] Pulling latest code from Github (origin main)...")
    stdin, stdout, stderr = client.exec_command("cd ~/cp_sim201 && git pull origin main 2>&1")
    print(stdout.read().decode().strip())
    
    print("\n[2] Running Unit tests on Board to ensure existing logic is intact...")
    stdin, stdout, stderr = client.exec_command("cd ~/cp_sim201 && export PYTHONPATH=$(pwd) && source venv/bin/activate && pytest tests/ -v 2>&1")
    print(stdout.read().decode().strip())
    
    print("\n[3] Testing main.py execution (Timeout 6s) to verify Board Sysfs GPIO bindings...")
    # Using sudo because /sys/class/gpio often requires root privileges
    cmd = f"cd ~/cp_sim201 && export PYTHONPATH=$(pwd) && source venv/bin/activate && echo '{pwd}' | sudo -S timeout 6 python3 src/main.py"
    stdin, stdout, stderr = client.exec_command(cmd)
    
    # Give the board time to execute, sleep and trigger timeout
    time.sleep(8)
    
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    
    print("--- [stdout] ---")
    print(out)
    print("--- [stderr (often includes sudo prompt)] ---")
    print(err)
         
except Exception as e:
    print(f"SSH Operation Failed: {e}")
finally:
    client.close()
