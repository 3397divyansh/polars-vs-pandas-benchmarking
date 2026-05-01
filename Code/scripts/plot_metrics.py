import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json
import os

sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)
plt.rcParams['savefig.dpi'] = 300 

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
REPORTS_DIR = os.path.join(DATA_DIR, "reports")

def load_k6_latency(filepath):
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            m = data['metrics']['http_req_duration{expected_response:true}']
            return [m['med'], m['p(90)'], m['p(95)'], m['p(99)']]
    except FileNotFoundError:
        print(f"Warning: {filepath} not found.")
        return [0, 0, 0, 0]
    except KeyError as e:
        print(f"Warning: Unexpected JSON structure in {filepath}. Error: {e}")
        return [0, 0, 0, 0]

def load_resource_data(filepath, engine_name):
    try:
        df = pd.read_csv(filepath)
        if df.empty:
            return df
        
        df = df[df['container'] == engine_name]
            
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        start_time = df['timestamp'].min()
        df['seconds_elapsed'] = (df['timestamp'] - start_time).dt.total_seconds()
        return df
    except FileNotFoundError as e:
        print(f"Warning: {filepath} not found.")
        return pd.DataFrame()

def plot_operation_metrics(operation: str):
    print(f"--- Generating Reports for Operation: {operation.upper()} ---")
    
    base_dir = REPORTS_DIR
    os.makedirs(base_dir, exist_ok=True)
    
    json_pd = os.path.join(base_dir, f'pandas_{operation}.json')
    json_pl = os.path.join(base_dir, f'polars_{operation}.json')
    csv_pd = os.path.join(base_dir, f'pandas_{operation}_resources.csv')
    csv_pl = os.path.join(base_dir, f'polars_{operation}_resources.csv')

    # 1. PLOT LATENCY (Bar Chart)
    pandas_stats = load_k6_latency(json_pd)
    polars_stats = load_k6_latency(json_pl)

    print(pandas_stats, polars_stats)

    labels = ['p50 (Median)', 'p90', 'p95', 'p99']
    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    rects1 = ax.bar(x - width/2, pandas_stats, width, label='Pandas (Sync)', color='#1f77b4')
    rects2 = ax.bar(x + width/2, polars_stats, width, label='Polars (Multithreaded)', color='#ff7f0e')

    ax.set_ylabel('Latency (milliseconds)', fontweight='bold')
    title_text = f'API Response Latency: {operation.replace("-", " ").title()}'
    ax.set_title(title_text, pad=20, fontweight='bold', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontweight='bold')
    ax.legend()

    ax.bar_label(rects1, padding=3, fmt='%.1f')
    ax.bar_label(rects2, padding=3, fmt='%.1f')

    plt.tight_layout()
    latency_out = os.path.join(base_dir, f'{operation}_latency.png')
    plt.savefig(latency_out)
    print(f"Saved Latency Chart: {latency_out}")
    plt.close()

    # 2. PLOT RESOURCES (CPU & Memory Line Charts)
    df_pd_res = load_resource_data(csv_pd, "benchmark-pandas")
    df_pl_res = load_resource_data(csv_pl, "benchmark-polars")

    if not df_pd_res.empty or not df_pl_res.empty:
        fig2, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
        fig2.suptitle(f'Resource Utilization: {operation.replace("-", " ").title()}', 
                      fontweight='bold', fontsize=14, y=0.98)

        if not df_pd_res.empty:
            sns.lineplot(data=df_pd_res, x='seconds_elapsed', y='cpu_percent', ax=ax1, label='Pandas CPU %', linewidth=2.5)
        if not df_pl_res.empty:
            sns.lineplot(data=df_pl_res, x='seconds_elapsed', y='cpu_percent', ax=ax1, label='Polars CPU %', linewidth=2.5)
            
        ax1.set_ylabel('CPU Usage (%)')
        ax1.axhline(200, ls='--', color='red', alpha=0.5, label='Max Allocation (2 Cores)')
        ax1.legend(loc='upper right')

        if not df_pd_res.empty:
            sns.lineplot(data=df_pd_res, x='seconds_elapsed', y='memory_mb', ax=ax2, label='Pandas RAM (MB)', linewidth=2.5)
        if not df_pl_res.empty:
            sns.lineplot(data=df_pl_res, x='seconds_elapsed', y='memory_mb', ax=ax2, label='Polars RAM (MB)', linewidth=2.5)
            
        ax2.set_xlabel('Time Elapsed (Seconds)', fontweight='bold')
        ax2.set_ylabel('Memory Usage (MB)')
        ax2.legend(loc='upper right')

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        resources_out = os.path.join(base_dir, f'{operation}_resources.png')
        plt.savefig(resources_out)
        print(f"Saved Resource Chart: {resources_out}")
        plt.close()
    else:
        print(f"Skipping resource charts for '{operation}': No CSV data found.")

if __name__ == "__main__":
    operation = sys.argv[1].lower() if len(sys.argv) > 1 else "heavy-join"
    plot_operation_metrics(operation)