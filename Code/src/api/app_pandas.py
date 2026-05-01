from fastapi import FastAPI, HTTPException
from typing import List

from src.schemas import (
    JoinSummary, AggregationSummary, TopCustomerSummary,
    DomainSummary, TimeSeriesSummary, ImputationSummary
)
from src.services import ops_pandas

app = FastAPI(
    title="Pandas E-Commerce Benchmark API",
    description="Synchronous Pandas execution of standard data engineering operations.",
    version="2.0.0"
)

@app.get("/benchmark/heavy-join", response_model=List[JoinSummary])
def get_heavy_join():
    """1. Executes a heavy multi-table inner join."""
    try:
        return ops_pandas.benchmark_heavy_join()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pandas processing error: {str(e)}")

@app.get("/benchmark/aggregations", response_model=List[AggregationSummary])
def get_aggregations():
    """2. Executes multi-dimensional groupings and unique counts."""
    try:
        return ops_pandas.benchmark_aggregations()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pandas processing error: {str(e)}")

@app.get("/benchmark/window-functions", response_model=List[TopCustomerSummary])
def get_window_functions():
    """3. Executes ranking and window functions (Top 3 per region)."""
    try:
        return ops_pandas.benchmark_window_functions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pandas processing error: {str(e)}")

@app.get("/benchmark/string-processing", response_model=List[DomainSummary])
def get_string_processing():
    """4. Executes text extraction and categorical grouping."""
    try:
        return ops_pandas.benchmark_string_processing()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pandas processing error: {str(e)}")

@app.get("/benchmark/time-series", response_model=List[TimeSeriesSummary])
def get_time_series():
    """5. Executes temporal resampling and rolling averages."""
    try:
        return ops_pandas.benchmark_time_series()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pandas processing error: {str(e)}")

@app.get("/benchmark/null-imputation", response_model=ImputationSummary)
def get_null_imputation():
    """6. Executes missing data detection and mean imputation."""
    try:
        return ops_pandas.benchmark_null_imputation()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pandas processing error: {str(e)}")
