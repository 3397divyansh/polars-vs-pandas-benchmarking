import subprocess
import time
import csv
import os
import sys
from datetime import datetime

engine_name = sys.argv[1].lower() if len(sys.argv) > 1 else "test"

OUTPUT_FILE = f"../data/reports/{engine_name}_resources.csv"
CONTAINERS = ["benchmark-pandas", "benchmark-polars"]

 

def get_docker_stats():
    stats = []
    for container in CONTAINERS:
        try:
            command = [
                "docker", "stats", container, 
                "--no-stream", 
                "--format", "{{.CPUPerc}},{{.MemUsage}}"
            ]
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            output = result.stdout.strip()
            
            if output:
                cpu_perc_str, mem_usage_str = output.split(",")
                cpu_perc = float(cpu_perc_str.replace("%", "").strip())
                current_mem = mem_usage_str.split("/")[0].strip()
                mem_mb = 0.0
                if "GiB" in current_mem:
                    mem_mb = float(current_mem.replace("GiB", "").strip()) * 1024
                elif "MiB" in current_mem:
                    mem_mb = float(current_mem.replace("MiB", "").strip())
                elif "kB" in current_mem:
                    mem_mb = float(current_mem.replace("kB", "").strip()) / 1024
                    
                stats.append({
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "container": container,
                    "cpu_percent": cpu_perc,
                    "memory_mb": round(mem_mb, 2)
                })
        except subprocess.CalledProcessError:
            print(f"Warning: Could not fetch stats for {container}. Is it running?")
        except Exception as e:
            print(f"Error parsing stats for {container}: {e}")
            
    return stats

def run_monitor(duration_seconds: int = 70):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    with open(OUTPUT_FILE, mode='w', newline='') as csv_file:
        fieldnames = ["timestamp", "container", "cpu_percent", "memory_mb"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        
        print(f"Starting Docker resource monitor for {duration_seconds} seconds...")
        print(f"Logging data to {OUTPUT_FILE}")
        
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            current_stats = get_docker_stats()
            for stat in current_stats:
                writer.writerow(stat)
                csv_file.flush() 
            
            time.sleep(0.5)
            
    print("Monitoring complete. Data saved.")

if __name__ == "__main__":
    run_monitor(duration_seconds=70)