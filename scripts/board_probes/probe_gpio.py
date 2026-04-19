import paramiko

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    print("Connecting...")
    client.connect(hostname=host, username=user, password=pwd, timeout=10)
    
    print("\n--- Listing /dev/gpiochip* ---")
    stdin, stdout, stderr = client.exec_command("ls -l /dev/gpiochip*")
    print(stdout.read().decode().strip())
    
    print("\n--- Reading /sys/kernel/debug/gpio (requires sudo) ---")
    stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S cat /sys/kernel/debug/gpio")
    print(stdout.read().decode().strip())

except Exception as e:
    print(f"Failed: {e}")
finally:
    client.close()
