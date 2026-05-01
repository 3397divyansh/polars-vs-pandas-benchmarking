from pydantic import BaseModel
from datetime import date
from typing import Optional

class JoinSummary(BaseModel):
    region: str
    category: str
    total_rows: int
    total_revenue: float

class AggregationSummary(BaseModel):
    region: str
    category: str
    unique_customers: int
    total_revenue: float
    average_discount: float

class TopCustomerSummary(BaseModel):
    region: str
    customer_id: str
    total_spend: float
    rank: int

class DomainSummary(BaseModel):
    email_domain: str
    total_revenue: float
    customer_count: int

class TimeSeriesSummary(BaseModel):
    week: date
    weekly_revenue: float
    rolling_30d_revenue: Optional[float]

class ImputationSummary(BaseModel):
    total_rows_processed: int
    nulls_filled: int
    original_mean_discount: float
    new_mean_discount: float