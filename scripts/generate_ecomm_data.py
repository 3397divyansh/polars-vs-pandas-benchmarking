# scripts/generate_ecomm_data.py
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
# Adjust these numbers if you want to test larger or smaller loads
NUM_CUSTOMERS = 100_000
NUM_PRODUCTS = 10_000
NUM_ORDERS = 1_000_000  # 5 Million rows is a great stress test

DATA_DIR = os.path.join(os.path.dirname(__name__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Set seed for 100% reproducible benchmarks
np.random.seed(42)

def generate_customers():
    print(f"Generating {NUM_CUSTOMERS:,} Customers...")
    
    customer_ids = [f"CUST_{i:07d}" for i in range(NUM_CUSTOMERS)]
    regions = ['North America', 'Europe', 'Asia', 'South America', 'Australia']
    tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'company.com', 'startup.io']
    
    # Vectorized random choice
    assigned_regions = np.random.choice(regions, NUM_CUSTOMERS, p=[0.4, 0.3, 0.15, 0.1, 0.05])
    
    # Inject 5% Nulls into the region column for our Imputation benchmark
    null_indices = np.random.choice(NUM_CUSTOMERS, size=int(NUM_CUSTOMERS * 0.05), replace=False)
    assigned_regions[null_indices] = None
    
    # Generate dates
    start_date = datetime(2020, 1, 1).timestamp()
    end_date = datetime(2025, 1, 1).timestamp()
    random_timestamps = np.random.randint(start_date, end_date, NUM_CUSTOMERS)
    creation_dates = pd.to_datetime(random_timestamps, unit='s')

    # Construct emails (simulating string processing needs)
    emails = [f"user_{i}@{np.random.choice(domains)}" for i in range(NUM_CUSTOMERS)]

    df = pd.DataFrame({
        'customer_id': customer_ids,
        'email_address': emails,
        'region': assigned_regions,
        'loyalty_tier': np.random.choice(tiers, NUM_CUSTOMERS, p=[0.5, 0.3, 0.15, 0.05]),
        'account_creation_date': creation_dates
    })
    
    path = os.path.join(DATA_DIR, "customers.parquet")
    df.to_parquet(path, index=False)
    print(f"Saved: {path}")
    return customer_ids

def generate_products():
    print(f"Generating {NUM_PRODUCTS:,} Products...")
    
    product_ids = [f"PROD_{i:05d}" for i in range(NUM_PRODUCTS)]
    categories = ['Electronics', 'Apparel', 'Home & Kitchen', 'Sports', 'Books']
    
    # Costs range from $5.00 to $500.00
    costs = np.round(np.random.uniform(5.0, 500.0, NUM_PRODUCTS), 2)
    
    df = pd.DataFrame({
        'product_id': product_ids,
        'category': np.random.choice(categories, NUM_PRODUCTS),
        'brand': [f"Brand_{np.random.randint(1, 100)}" for _ in range(NUM_PRODUCTS)],
        'manufacturing_cost': costs
    })
    
    path = os.path.join(DATA_DIR, "products.parquet")
    df.to_parquet(path, index=False)
    print(f"Saved: {path}")
    return product_ids, df

def generate_orders(customer_ids, products_df):
    print(f"Generating {NUM_ORDERS:,} Orders (This might take a few seconds)...")
    
    order_ids = [f"ORD_{i:08d}" for i in range(NUM_ORDERS)]
    
    # Randomly assign customers and products
    order_customers = np.random.choice(customer_ids, NUM_ORDERS)
    order_products = np.random.choice(products_df['product_id'].values, NUM_ORDERS)
    
    # Generate random order timestamps over the last 3 years
    start_date = datetime(2023, 1, 1).timestamp()
    end_date = datetime(2026, 1, 1).timestamp()
    random_timestamps = np.random.randint(start_date, end_date, NUM_ORDERS)
    order_dates = pd.to_datetime(random_timestamps, unit='s')
    
    # Simulate pricing logic based on product cost
    # We map the product ID back to its cost, then add a random markup to get unit_price
    product_cost_map = dict(zip(products_df['product_id'], products_df['manufacturing_cost']))
    base_costs = np.array([product_cost_map[pid] for pid in order_products])
    markups = np.random.uniform(1.2, 2.5, NUM_ORDERS)
    unit_prices = np.round(base_costs * markups, 2)
    
    # Quantities between 1 and 5
    quantities = np.random.randint(1, 6, NUM_ORDERS)
    
    # Discounts (0% to 30%)
    discounts = np.round(np.random.uniform(0.0, 0.30, NUM_ORDERS), 2)
    
    # Inject 10% Nulls into the discount column for our Imputation benchmark
    null_indices = np.random.choice(NUM_ORDERS, size=int(NUM_ORDERS * 0.10), replace=False)
    discounts[null_indices] = np.nan

    df = pd.DataFrame({
        'order_id': order_ids,
        'customer_id': order_customers,
        'product_id': order_products,
        'order_timestamp': order_dates,
        'quantity': quantities,
        'unit_price': unit_prices,
        'discount_percent': discounts
    })
    
    path = os.path.join(DATA_DIR, "orders.parquet")
    df.to_parquet(path, index=False)
    print(f"Saved: {path}")

if __name__ == "__main__":
    print("--- Starting E-Commerce Data Generation ---")
    c_ids = generate_customers()
    p_ids, p_df = generate_products()
    generate_orders(c_ids, p_df)
    print("--- Data Generation Complete! ---")