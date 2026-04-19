import paramiko

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

script = """
import os
import time

for chip in ['pwmchip0', 'pwmchip4']:
    pwm_dir = f'/sys/class/pwm/{chip}/pwm0'
    print(f"\\nTesting {chip}...")
    try:
        with open(f'/sys/class/pwm/{chip}/export', 'w') as f:
            f.write('0')
        print("  [OK] Exported")
    except Exception as e:
        print(f"  Export exception (might be already exported): {e}")

    time.sleep(0.1)

    try:
        with open(f'{pwm_dir}/period', 'r') as f: print(f"  Current Period: {f.read().strip()}")
        with open(f'{pwm_dir}/duty_cycle', 'r') as f: print(f"  Current Duty: {f.read().strip()}")
    except Exception as e:
        print(f"  Read error: {e}")

    steps = [
        ('duty_cycle', '0'),
        ('period', '1000000'),
        ('duty_cycle', '500000'),
        ('enable', '1')
    ]
    for filename, val in steps:
        try:
            with open(f'{pwm_dir}/{filename}', 'w') as f:
                f.write(val)
            print(f"  [OK] Wrote {val} to {filename}")
        except Exception as e:
            print(f"  [FAIL] Writing {val} to {filename}: {e}")
"""

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(host, username=user, password=pwd, timeout=5)

sftp = client.open_sftp()
with sftp.file('/tmp/test_pwm_err.py', 'w') as f:
    f.write(script)

stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S python3 /tmp/test_pwm_err.py")
print(stdout.read().decode())
err = stderr.read().decode()
if err:
    print("STDERR:\\n", err)
client.close()
