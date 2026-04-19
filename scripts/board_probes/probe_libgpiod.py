import paramiko
import time

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    client.connect(hostname=host, username=user, password=pwd, timeout=10)
    
    py_cmd = """
import ctypes
import sys
try:
    lib = ctypes.CDLL("libgpiod.so.3")
    print("Loaded libgpiod.so.3 successfully via CDLL")
    if hasattr(lib, 'gpiod_chip_open'):
        print("Has gpiod_chip_open")
    if hasattr(lib, 'gpiod_request_config_new'):
        print("Detected libgpiod v2 ABI (gpiod_request_config_new)")
    elif hasattr(lib, 'gpiod_line_request_output'):
        print("Detected libgpiod v1 ABI (gpiod_line_request_output)")
    else:
        print("Unknown ABI / Missing line request functions")
except Exception as e:
    print(f"Failed to load: {e}")
    sys.exit(1)
"""
    print("\n[Probing libgpiod ABI on Target]")
    stdin, stdout, stderr = client.exec_command(f"python3 -c '{py_cmd}'")
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    print("STDOUT:", out)
    print("STDERR:", err)
         
except Exception as e:
    print(f"SSH Operation Failed: {e}")
finally:
    client.close()
