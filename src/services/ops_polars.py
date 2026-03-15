# src/services/ops_polars.py
import polars as pl
from src.config import settings
from src.schemas import (
    JoinSummary, AggregationSummary, TopCustomerSummary, 
    DomainSummary, TimeSeriesSummary, ImputationSummary
)

# --- 1. Load Data into Memory (The Cache) ---
# We load this once at startup to ensure a fair CPU/Memory comparison against Pandas.
print("Polars Service: Loading Parquet files into memory...")
try:
    orders_cache = pl.read_parquet(settings.ORDERS_PATH)
    customers_cache = pl.read_parquet(settings.CUSTOMERS_PATH)
    products_cache = pl.read_parquet(settings.PRODUCTS_PATH)
    print(f"Loaded: {orders_cache.height:,} Orders, {customers_cache.height:,} Customers, {products_cache.height:,} Products.")
except Exception as e:
    print(f"Warning: Could not load data. Did you run the generation script? Error: {e}")
    orders_cache, customers_cache, products_cache = pl.DataFrame(), pl.DataFrame(), pl.DataFrame()


# --- 2. The 6 Benchmark Operations ---

def benchmark_heavy_join() -> list[JoinSummary]:
    """1. The Heavy Relational Join"""
    # Initialize lazy execution graphs
    q_orders = orders_cache.lazy()
    q_customers = customers_cache.lazy()
    q_products = products_cache.lazy()
    
    # Polars automatically optimizes this join block to use minimum memory
    q = (
        q_orders
        .join(q_customers, on="customer_id", how="inner")
        .join(q_products, on="product_id", how="inner")
        .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("revenue"))
        .group_by(["region", "category"])
        .agg([
            pl.len().alias("total_rows"),
            pl.col("revenue").sum().alias("total_revenue")
        ])
    )
    
    # .collect() executes the multithreaded graph
    return [JoinSummary(**row) for row in q.collect().to_dicts()]


def benchmark_aggregations() -> list[AggregationSummary]:
    """2. Multi-Dimensional Aggregations"""
    q_orders = orders_cache.lazy()
    q_customers = customers_cache.lazy()
    q_products = products_cache.lazy()
    
    q = (
        q_orders
        .join(q_customers, on="customer_id", how="inner")
        .join(q_products, on="product_id", how="inner")
        .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("revenue"))
        .group_by(["region", "category"])
        .agg([
            # Native n_unique in Polars is blisteringly fast compared to Pandas
            pl.col("customer_id").n_unique().alias("unique_customers"),
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("discount_percent").mean().fill_null(0.0).alias("average_discount")
        ])
    )
    
    return [AggregationSummary(**row) for row in q.collect().to_dicts()]


def benchmark_window_functions() -> list[TopCustomerSummary]:
    """3. Window Functions (Top 3 Customers per Region)"""
    q_orders = orders_cache.lazy()
    q_customers = customers_cache.lazy()
    
    q = (
        q_orders
        .join(q_customers, on="customer_id", how="inner")
        .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("spend"))
        .group_by(["region", "customer_id"])
        .agg(pl.col("spend").sum().alias("total_spend"))
        # Native Window Function (.over) - No slow Python group_by.apply() needed
        .with_columns(
            pl.col("total_spend").rank(method="dense", descending=True).over("region").alias("rank")
        )
        .filter(pl.col("rank") <= 3)
        .sort(["region", "rank"])
    )
    
    return [TopCustomerSummary(**row) for row in q.collect().to_dicts()]


def benchmark_string_processing() -> list[DomainSummary]:
    """4. String Manipulation & Feature Engineering"""
    q_orders = orders_cache.lazy()
    q_customers = customers_cache.lazy()
    
    # Polars string processing is backed by Apache Arrow (C++), completely bypassing Python overhead
    q_customers = q_customers.with_columns(
        pl.col("email_address").str.split("@").list.last().alias("email_domain")
    )
    
    q = (
        q_orders
        .join(q_customers, on="customer_id", how="inner")
        .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("revenue"))
        .group_by("email_domain")
        .agg([
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("customer_id").n_unique().alias("customer_count")
        ])
    )
    
    return [DomainSummary(**row) for row in q.collect().to_dicts()]


def benchmark_time_series() -> list[TimeSeriesSummary]:
    """5. Time-Series Resampling & Rolling Windows"""
    # Polars requires the datetime column to be sorted for dynamic grouping
    q = (
        orders_cache.lazy()
        .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("revenue"))
        .sort("order_timestamp")
        # Step 1: Resample to daily
        .group_by_dynamic("order_timestamp", every="1d")
        .agg(pl.col("revenue").sum())
        # Step 2: 30-day rolling window
        .with_columns(
            pl.col("revenue").rolling_mean(window_size=30, min_periods=1).fill_null(0.0).alias("rolling_30d_revenue")
        )
        # Step 3: Downsample to weekly
        .group_by_dynamic("order_timestamp", every="1w")
        .agg([
            pl.col("revenue").sum().alias("weekly_revenue"),
            pl.col("rolling_30d_revenue").last()
        ])
        .with_columns(pl.col("order_timestamp").dt.date().alias("week"))
    )
    
    return [TimeSeriesSummary(**row) for row in q.collect().to_dicts()]


def benchmark_null_imputation() -> ImputationSummary:
    """6. The 'Messy Data' Imputation"""
    # We can calculate all these metrics in a single pass over the data
    # unlike Pandas which requires multiple scans.
    q = orders_cache.lazy()
    
    result = q.select([
        pl.len().alias("total_rows_processed"),
        pl.col("discount_percent").null_count().alias("nulls_filled"),
        pl.col("discount_percent").mean().alias("original_mean_discount"),
        pl.col("discount_percent").fill_null(pl.col("discount_percent").mean()).mean().alias("new_mean_discount")
    ]).collect().to_dicts()[0]
    
    return ImputationSummary(**result)