import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR: str = os.path.join(BASE_DIR, "data")
    
    ORDERS_PATH: str = os.path.join(DATA_DIR, "orders.parquet")
    CUSTOMERS_PATH: str = os.path.join(DATA_DIR, "customers.parquet")
    PRODUCTS_PATH: str = os.path.join(DATA_DIR, "products.parquet")

settings = Settings()
