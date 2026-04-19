import paramiko

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'
try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=5)
    
    cmds = [
        "ls -l /sys/class/pwm",
        "dmesg | grep -i pwm"
    ]
    for cmd in cmds:
        print(f"--- {cmd} ---")
        _, stdout, stderr = client.exec_command(cmd)
        out = stdout.read().decode().strip()
        if out: print(out)
        err = stderr.read().decode().strip()
        if err: print("ERR:", err)
        
    client.close()
except Exception as e:
    print(f"Error: {e}")
