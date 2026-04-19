import paramiko

host = '192.168.0.102'
user = 'jeongsooh'
pwd = 'glrtma311'

script = """
import os
import time

chip = 'pwmchip0'

print("--- PWM Diagnostics for TIM2 (pwmchip0) ---")

try:
    with open(f'/sys/class/pwm/{chip}/npwm', 'r') as f:
        print(f"npwm: {f.read().strip()} channels available")
except Exception as e:
    print("npwm read error:", e)

# Test channel 2 (which corresponds to CH3 zero-indexed)
chan = '2'
try:
    with open(f'/sys/class/pwm/{chip}/export', 'w') as f:
        f.write(chan)
    time.sleep(0.2)
except Exception as e:
    pass # Usually throws Error 16 if already exported

pwm_dir = f'/sys/class/pwm/{chip}/pwm{chan}'

if os.path.exists(pwm_dir):
    print(f"[OK] {pwm_dir} exists! Writing test wave...")
    try:
        with open(f"{pwm_dir}/duty_cycle", "r") as f:
            curr_duty = int(f.read().strip())
        
        if curr_duty > 1000000:
            with open(f"{pwm_dir}/duty_cycle", "w") as f:
                f.write("0")
                
        with open(f'{pwm_dir}/period', 'w') as f: f.write('1000000')   # 1 kHz
        with open(f'{pwm_dir}/duty_cycle', 'w') as f: f.write('500000') # 50% duty
        with open(f'{pwm_dir}/enable', 'w') as f: f.write('1')
        print(f"[SUCCESS] Wrote 50% (500000ns) duty at 1kHz to {chip} channel {chan}")
    except Exception as e:
        print(f"[FAIL] Writing to {chip} channel {chan}: {e}")
else:
    print(f"[FAIL] {pwm_dir} does not exist after export attempt.")
"""

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(host, username=user, password=pwd, timeout=5)

sftp = client.open_sftp()
with sftp.file('/tmp/test_tim2.py', 'w') as f:
    f.write(script)

stdin, stdout, stderr = client.exec_command(f"echo '{pwd}' | sudo -S python3 /tmp/test_tim2.py")
print(stdout.read().decode())
err = stderr.read().decode()
if err: print("STDERR:", err)
client.close()
