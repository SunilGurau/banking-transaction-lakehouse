from __future__ import annotations

from airflow.decorators import dag, task
from airflow.utils import timezone


@dag(
    dag_id="hello_airflow_foundation",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["foundation", "handson"],
)
def hello_airflow_foundation():
    @task
    def extract():
        print("Extracting data from source system...")
        return {"orders": 120, "customers": 35}

    @task
    def transform(raw_data: dict):
        print(f"Transforming raw data: {raw_data}")
        return {
            "clean_orders": raw_data["orders"],
            "active_customers": raw_data["customers"],
        }

    @task
    def load(clean_data: dict):
        print(f"loading clean data into warehouse: {clean_data}")

    load(transform(extract()))


hello_airflow_foundation_dag = hello_airflow_foundation()
