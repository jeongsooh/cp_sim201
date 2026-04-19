import paramiko

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

try:
    print(f"Connecting to {host}...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=pwd, timeout=10)

    commands = [
        "dmesg | grep -i tty",
        "ls -l /dev/ttySTM*",
        "echo 'glrtma311' | sudo -S cat /proc/tty/driver/stm32-usart"
    ]

    for cmd in commands:
        print(f"\n--- CMD: {cmd} ---")
        stdin, stdout, stderr = client.exec_command(cmd)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        if out: print(out)
        if err: print(f"ERROR: {err}")

    client.close()
    print("\nConnection closed.")
except Exception as e:
    print(f"SSH Error: {e}")
