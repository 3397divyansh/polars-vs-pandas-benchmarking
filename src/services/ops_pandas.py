# src/services/ops_pandas.py
import pandas as pd
import numpy as np
from src.config import settings
from src.schemas import (
    JoinSummary, AggregationSummary, TopCustomerSummary, 
    DomainSummary, TimeSeriesSummary, ImputationSummary
)

# --- 1. Load Data into Memory (The Cache) ---
# We load this once at startup to isolate CPU/Memory performance from Disk I/O.
print("Pandas Service: Loading Parquet files into memory...")
try:
    orders_cache = pd.read_parquet(settings.ORDERS_PATH)
    customers_cache = pd.read_parquet(settings.CUSTOMERS_PATH)
    products_cache = pd.read_parquet(settings.PRODUCTS_PATH)
    print(f"Loaded: {len(orders_cache):,} Orders, {len(customers_cache):,} Customers, {len(products_cache):,} Products.")
except Exception as e:
    print(f"Warning: Could not load data. Did you run the generation script? Error: {e}")
    orders_cache, customers_cache, products_cache = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# --- 2. The 6 Benchmark Operations ---

def benchmark_heavy_join() -> list[JoinSummary]:
    """1. The Heavy Relational Join"""
    # Create a fresh copy/reference of the needed columns to avoid mutating the cache
    orders = orders_cache[['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price']]
    customers = customers_cache[['customer_id', 'region']]
    products = products_cache[['product_id', 'category']]
    
    # Perform the heavy merges (Pandas creates intermediate copies here in RAM)
    merged = orders.merge(customers, on='customer_id', how='inner')
    merged = merged.merge(products, on='product_id', how='inner')
    
    # Calculate revenue
    merged['revenue'] = merged['quantity'] * merged['unit_price']
    
    # Group and aggregate
    grouped = merged.groupby(['region', 'category']).agg(
        total_rows=('order_id', 'count'),
        total_revenue=('revenue', 'sum')
    ).reset_index()
    
    return [JoinSummary(**row) for row in grouped.to_dict(orient='records')]


def benchmark_aggregations() -> list[AggregationSummary]:
    """2. Multi-Dimensional Aggregations"""
    # Merge needed for region/category context
    merged = orders_cache.merge(customers_cache[['customer_id', 'region']], on='customer_id', how='inner')
    merged = merged.merge(products_cache[['product_id', 'category']], on='product_id', how='inner')
    
    merged['revenue'] = merged['quantity'] * merged['unit_price']
    
    # Pandas 'nunique' is notoriously slow. This is a great stress test.
    grouped = merged.groupby(['region', 'category']).agg(
        unique_customers=('customer_id', 'nunique'),
        total_revenue=('revenue', 'sum'),
        average_discount=('discount_percent', 'mean')
    ).reset_index()
    
    # Handle possible NaNs in mean calculations gracefully
    grouped['average_discount'] = grouped['average_discount'].fillna(0.0)
    
    return [AggregationSummary(**row) for row in grouped.to_dict(orient='records')]


def benchmark_window_functions() -> list[TopCustomerSummary]:
    """3. Window Functions (Top 3 Customers per Region)"""
    merged = orders_cache.merge(customers_cache[['customer_id', 'region']], on='customer_id', how='inner')
    merged['spend'] = merged['quantity'] * merged['unit_price']
    
    # Step 1: Total spend per customer
    customer_spend = merged.groupby(['region', 'customer_id'])['spend'].sum().reset_index()
    customer_spend.rename(columns={'spend': 'total_spend'}, inplace=True)
    
    # Step 2: Window function (Rank within Region)
    customer_spend['rank'] = customer_spend.groupby('region')['total_spend'].rank(method='dense', ascending=False)
    
    # Step 3: Filter for Top 3
    top_3 = customer_spend[customer_spend['rank'] <= 3.0].sort_values(['region', 'rank'])
    
    return [TopCustomerSummary(**row) for row in top_3.to_dict(orient='records')]


def benchmark_string_processing() -> list[DomainSummary]:
    """4. String Manipulation & Feature Engineering"""
    customers = customers_cache.copy()
    orders = orders_cache[['customer_id', 'quantity', 'unit_price']]
    
    # String splitting in Pandas usually drops down to slow Python-level loops
    customers['email_domain'] = customers['email_address'].str.split('@').str[1]
    
    merged = orders.merge(customers[['customer_id', 'email_domain']], on='customer_id', how='inner')
    merged['revenue'] = merged['quantity'] * merged['unit_price']
    
    grouped = merged.groupby('email_domain').agg(
        total_revenue=('revenue', 'sum'),
        customer_count=('customer_id', 'nunique')
    ).reset_index()
    
    return [DomainSummary(**row) for row in grouped.to_dict(orient='records')]


def benchmark_time_series() -> list[TimeSeriesSummary]:
    """5. Time-Series Resampling & Rolling Windows"""
    orders = orders_cache[['order_timestamp', 'quantity', 'unit_price']].copy()
    orders['revenue'] = orders['quantity'] * orders['unit_price']
    
    # Pandas requires the datetime column to be the index for rolling/resampling
    orders.set_index('order_timestamp', inplace=True)
    
    # Resample to daily frequency first
    daily = orders.resample('D')['revenue'].sum().reset_index()
    
    # Calculate 30-day rolling average
    daily['rolling_30d_revenue'] = daily['revenue'].rolling(window=30, min_periods=1).mean()
    
    # Downsample to weekly for the final report
    daily.set_index('order_timestamp', inplace=True)
    weekly = daily.resample('W').agg({
        'revenue': 'sum',
        'rolling_30d_revenue': 'last' # Take the state of the rolling average at the end of the week
    }).reset_index()
    
    # Rename for schema compliance
    weekly.rename(columns={'order_timestamp': 'week', 'revenue': 'weekly_revenue'}, inplace=True)
    # Convert timestamp to date object for JSON serialization
    weekly['week'] = weekly['week'].dt.date 
    
    # Fill any NaNs resulting from rolling calculation
    weekly['rolling_30d_revenue'] = weekly['rolling_30d_revenue'].fillna(0.0)
    
    return [TimeSeriesSummary(**row) for row in weekly.to_dict(orient='records')]


def benchmark_null_imputation() -> ImputationSummary:
    """6. The 'Messy Data' Imputation"""
    orders = orders_cache[['discount_percent']].copy()
    total_rows = len(orders)
    
    # Count nulls before imputation
    nulls_to_fill = orders['discount_percent'].isna().sum()
    
    # Calculate original mean (ignoring nulls by default in Pandas)
    original_mean = orders['discount_percent'].mean()
    
    # Impute missing values with the mean
    orders['discount_percent'] = orders['discount_percent'].fillna(original_mean)
    
    # Verify new mean (should be mathematically identical, tests processing logic)
    new_mean = orders['discount_percent'].mean()
    
    return ImputationSummary(
        total_rows_processed=total_rows,
        nulls_filled=int(nulls_to_fill),
        original_mean_discount=float(original_mean),
        new_mean_discount=float(new_mean)
    )