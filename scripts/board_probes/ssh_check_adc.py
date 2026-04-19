import paramiko

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

script = """
import os

print("--- ADC Diagnostics for IIO Subsystem ---")

iio_path = "/sys/bus/iio/devices"
if not os.path.exists(iio_path):
    print(f"[FAIL] {iio_path} does not exist. ADC might be disabled in kernel.")
else:
    devices = os.listdir(iio_path)
    if not devices:
        print("[FAIL] Empty IIO path. No ADCs enabled in Device Tree.")
    for dev in devices:
        if dev.startswith("iio:device"):
            dev_path = os.path.join(iio_path, dev)
            print(f"\\n--- {dev} ---")
            
            # Read name
            try:
                with open(os.path.join(dev_path, "name"), "r") as f:
                    print(f"Name: {f.read().strip()}")
            except:
                pass
                
            channels = [f for f in os.listdir(dev_path) if f.startswith("in_voltage") and f.endswith("_raw")]
            if channels:
                print(f"Active Channels: {len(channels)}")
                for ch in sorted(channels):
                    try:
                        with open(os.path.join(dev_path, ch), "r") as f:
                            print(f"  {ch}: {f.read().strip()}")
                    except Exception as e:
                        print(f"  {ch}: [ERROR] {e}")
            else:
                print("No in_voltage_raw channels found.")
"""

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(host, username=user, password=pwd, timeout=5)

sftp = client.open_sftp()
with sftp.file('/tmp/test_adc.py', 'w') as f:
    f.write(script)

stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S python3 /tmp/test_adc.py")
print(stdout.read().decode())
err = stderr.read().decode()
if "password for" not in err.lower() and err.strip():
    print("STDERR:\\n", err)
client.close()
