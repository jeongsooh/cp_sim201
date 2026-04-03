import os
import sys
import time

def setup_pwm(chip, channel=0):
    export_path = f"/sys/class/pwm/{chip}/export"
    pwm_dir = f"/sys/class/pwm/{chip}/pwm{channel}"
    
    if not os.path.exists(pwm_dir):
        try:
            with open(export_path, "w") as f:
                f.write(str(channel))
            time.sleep(0.1) # Wait for udev to set permissions
        except Exception as e:
            print(f"[!] Failed to export {chip} (Channel {channel}): {e}")
            return False
    return True

def set_pwm(chip, channel, period_ns, duty_ns):
    pwm_dir = f"/sys/class/pwm/{chip}/pwm{channel}"
    try:
        with open(f"{pwm_dir}/duty_cycle", "r") as f:
            curr_duty = int(f.read().strip())
            
        # Linux sysfs kernel constraints: duty_cycle cannot exceed period.
        # If current duty is greater than the TARGET period, it would clash when writing period.
        # So we temporarily zero the duty cycle first.
        if curr_duty > period_ns:
            with open(f"{pwm_dir}/duty_cycle", "w") as f:
                f.write("0")
        
        with open(f"{pwm_dir}/period", "w") as f:
            f.write(str(period_ns))
            
        with open(f"{pwm_dir}/duty_cycle", "w") as f:
            f.write(str(duty_ns))
            
        with open(f"{pwm_dir}/enable", "w") as f:
            f.write("1")
        return True
    except Exception as e:
        print(f"[!] Error applying PWM to {chip}: {e}")
        return False

def main():
    print("==========================================")
    print("   STM32 Control Pilot (CP) PWM Tester    ")
    print("==========================================")
    
    chips = ["pwmchip0", "pwmchip4"]
    active_chips = []
    
    for chip in chips:
        if os.path.exists(f"/sys/class/pwm/{chip}"):
            if setup_pwm(chip):
                active_chips.append(chip)
                print(f"[OK] {chip} is ready.")
        else:
            print(f"[--] {chip} not found.")

    if not active_chips:
        print("[!] No hardware PWM chips found. Exiting.")
        sys.exit(1)

    print("\n[INFO] Both active chips will mirror the identical PWM settings.")
    print("[INFO] Please attach your Oscilloscope/Multimeter to the CP Pin.")
    print("[INFO] The period is permanently locked to 1 kHz (1,000,000 ns).\n")

    PERIOD_NS = 1000000

    try:
        while True:
            val = input("Enter target Duty Cycle (0 to 100) or 'q' to quit: ").strip()
            if val.lower() == 'q':
                break
                
            try:
                duty_percent = int(val)
                if not (0 <= duty_percent <= 100):
                    raise ValueError
            except ValueError:
                print("Please enter a valid integer between 0 and 100.")
                continue

            duty_ns = int(PERIOD_NS * (duty_percent / 100.0))
            
            for chip in active_chips:
                success = set_pwm(chip, 0, PERIOD_NS, duty_ns)
                if success:
                    print(f" -> {chip} Output: 1kHz @ {duty_percent}% Duty ({duty_ns}ns)")
                    
    except KeyboardInterrupt:
        pass
    print("\nExiting CP Tester. (PWM will remain generated until reboot or disabled)")

if __name__ == "__main__":
    main()
