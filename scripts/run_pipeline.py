# scripts/run_pipeline.py
import os
import subprocess
import time
import sys

# Define the 6 operations we want to benchmark
OPERATIONS = [
    "heavy-join",
    "aggregations",
    "window-functions",
    "string-processing",
    "time-series",
    "null-imputation"
]
OPERATIONS_FILE = [
    "heavy_join",
    "aggregations",
    "window_functions",
    "string_processing",
    "time_series",
    "null_imputation"
]


# Set up robust absolute paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
REPORTS_DIR = os.path.join(PROJECT_ROOT, "data", "reports")

# Ensure the reports directory exists
os.makedirs(REPORTS_DIR, exist_ok=True)

def run_pipeline():
    print("=" * 60)
    print("🚀 STARTING AUTOMATED BENCHMARK PIPELINE")
    print("=" * 60)
    
    for i in range(len(OPERATIONS)):
        op = OPERATIONS[i]
        op_file = OPERATIONS_FILE[i]
        print(f"\n\n{'='*60}")
        print(f"📊 PHASE: {op.upper()}")
        print(f"{'='*60}")
        
        # 1. Start the resource monitor in the background
        print(f"[{op}] Starting resource monitor (Background Process)...")
        monitor_script = os.path.join(SCRIPT_DIR, "monitor_resources.py")
        # We pass 150 seconds to ensure it outlives both 60-second k6 tests + cooldowns
        monitor_proc = subprocess.Popen([sys.executable, monitor_script, f"pandas_{op_file}", "150"])
        
        # Give the monitor 2 seconds to initialize its CSV files
        time.sleep(2) 
        
        # 2. Run k6 for Pandas
        print(f"\n[{op}] 🔴 Firing k6 load test against Pandas (Port 8000)...")
        pandas_json = os.path.join(REPORTS_DIR, f"pandas_{op_file}.json")
        k6_script = os.path.join(PROJECT_ROOT, "tests", "load_test.js")
        
        pandas_cmd = [
            "k6", "run",
            "-e", f"TARGET_URL=http://localhost:8000/benchmark/{op}",
            f"--summary-export={pandas_json}",
            k6_script
        ]
        # We use check=False so if a container crashes, the pipeline continues to the next test
        subprocess.run(pandas_cmd, check=False)
        
        # 3. Cooldown
        print(f"\n[{op}] ⏳ Cooling down for 5 seconds to let CPU/RAM settle...")
        time.sleep(5)

        print(f"\n[{op}] Stopping resource monitor for pandas...")
        monitor_proc.terminate()
        monitor_proc.wait()


        monitor_proc = subprocess.Popen([sys.executable, monitor_script, f"polars_{op_file}", "150"])

        time.sleep(2) 
        # 4. Run k6 for Polars
        print(f"\n[{op}] 🟠 Firing k6 load test against Polars (Port 8001)...")
        polars_json = os.path.join(REPORTS_DIR, f"polars_{op_file}.json")
        
        polars_cmd = [
            "k6", "run",
            "-e", f"TARGET_URL=http://localhost:8001/benchmark/{op}",
            f"--summary-export={polars_json}",
            k6_script
        ]
        subprocess.run(polars_cmd, check=False)
        
        # 5. Stop the monitor early (since the tests are done)
        time.sleep(5)
        print(f"\n[{op}] Stopping resource monitor for polars...")
        monitor_proc.terminate()
        monitor_proc.wait()
        
        # 6. Generate the Graphs
        print(f"[{op}] 📈 Generating performance graphs...")
        plot_script = os.path.join(SCRIPT_DIR, "plot_metrics.py")
        subprocess.run([sys.executable, plot_script, op_file], check=False)
        
        print(f"[{op}] ✅ Operation Complete!")

    print("\n\n" + "=" * 60)
    print("🎉 PIPELINE COMPLETE! All graphs saved to data/reports/")
    print("=" * 60)

if __name__ == "__main__":
    run_pipeline()