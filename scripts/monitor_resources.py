# scripts/monitor_resources.py
import subprocess
import time
import csv
import os
import sys
from datetime import datetime

# Grab the engine name from the terminal argument (default to 'test' if forgotten)
engine_name = sys.argv[1].lower() if len(sys.argv) > 1 else "test"

# Dynamically name the output file
OUTPUT_FILE = f"../data/reports/{engine_name}_resources.csv"
CONTAINERS = ["benchmark-pandas", "benchmark-polars"]

 

def get_docker_stats():
    """
    Calls the docker CLI to get the current CPU and Memory usage 
    for the specified containers.
    """
    stats = []
    for container in CONTAINERS:
        try:
            # We use --no-stream to get a single snapshot, and --format to output clean CSV data
            command = [
                "docker", "stats", container, 
                "--no-stream", 
                "--format", "{{.CPUPerc}},{{.MemUsage}}"
            ]
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            output = result.stdout.strip()
            
            if output:
                # Example output: "105.50%, 450MiB / 2GiB"
                cpu_perc_str, mem_usage_str = output.split(",")
                
                # Clean up the strings to get raw numbers
                cpu_perc = float(cpu_perc_str.replace("%", "").strip())
                
                # Extract just the current usage (ignore the limit part after the slash)
                current_mem = mem_usage_str.split("/")[0].strip()
                
                # Convert MiB or GiB to a standard float (Megabytes)
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
    """
    Runs the monitoring loop for slightly longer than our k6 test 
    to capture the full ramp-up and ramp-down.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Open the CSV file and write the headers
    with open(OUTPUT_FILE, mode='w', newline='') as csv_file:
        fieldnames = ["timestamp", "container", "cpu_percent", "memory_mb"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        
        print(f"Starting Docker resource monitor for {duration_seconds} seconds...")
        print(f"Logging data to {OUTPUT_FILE}")
        
        start_time = time.time()
        
        # Loop until the duration is met
        while time.time() - start_time < duration_seconds:
            current_stats = get_docker_stats()
            for stat in current_stats:
                writer.writerow(stat)
                # Flush immediately so data isn't lost if we cancel the script
                csv_file.flush() 
            
            # Pause for 1 second before polling again
            time.sleep(0.5)
            
    print("Monitoring complete. Data saved.")

if __name__ == "__main__":
    # Our k6 test runs for 60 seconds total (10s ramp + 40s hold + 10s down)
    # We run this for 70 seconds to ensure we capture the full window.
    run_monitor(duration_seconds=70)