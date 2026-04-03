import os
import sys
import time

def main():
    print("==========================================")
    print("      STM32 ADC Voltage Reader Test       ")
    print("==========================================")
    
    adc_dir = "/sys/bus/iio/devices/iio:device2"
    if not os.path.exists(adc_dir):
        print(f"[!] ADC device NOT found at {adc_dir}")
        sys.exit(1)
        
    channels = ["in_voltage0_raw", "in_voltage1_raw", "in_voltage6_raw"]
    
    for ch in channels:
        if not os.path.exists(os.path.join(adc_dir, ch)):
            print(f"[!] Warning: Channel {ch} completely missing.")

    print("\n[INFO] Live monitoring ADC channels...")
    print("[INFO] Try inserting/removing the EV connector and watch the values!")
    print("-----" * 10)
    
    try:
        while True:
            vals = []
            for ch in channels:
                try:
                    with open(os.path.join(adc_dir, ch), "r") as f:
                        vals.append(f"{ch[-5]}: {f.read().strip().rjust(5)}")
                except:
                    vals.append(f"{ch[-5]}: ERROR")
                    
            sys.stdout.write("\r" + " | ".join(vals))
            sys.stdout.flush()
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n\nExiting ADC Tester.")

if __name__ == "__main__":
    main()
