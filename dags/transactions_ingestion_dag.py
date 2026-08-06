# from __future__ import annotations

# import sys
# from pathlib import Path

# from airflow.decorators import dag, task
# from airflow.utils import timezone

# SRC_DIR = Path(__file__).resolve().parents[1] / "src"
# if str(SRC_DIR) not in sys.path:
#     sys.path.insert(0, str(SRC_DIR))


# @dag(
#     dag_id="transactions_ingestion",
#     start_date=timezone.datetime(2026, 1, 29),
#     schedule="0 0 * * *",
#     catchup=True,
#     tags=["batch", "ingestion", "transactions"],
# )
# def transactions_ingestion(dag_run=None):
#     @task
#     def load_transactions():

#         logical_date = dag_run.logical_date.date()
#         print(f"Loading transactions for logical date: {logical_date}")
#         folder = Path("/opt/airflow/data/batch/transactions")

#         file_path = folder / f"transactions_{logical_date}.csv"
#         print(f"Loading transactions from file: {file_path}")
#         # load_reference_csvs_to_minio(file_path, prefix="transactions")

#     load_transactions()


# transactions_ingestion_dag = transactions_ingestion()


# ----------------------------------------------------------


import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from airflow.decorators import dag, task
from airflow.utils import timezone

from reference_minio_loader import load_reference_csvs_to_minio


@dag(
    dag_id="transactions_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=True,
    tags=["batch", "ingestion", "transactions"],
    max_active_runs=1,
)
def transactions_ingestion():

    @task
    def load_transactions(**kwargs):
        logical_date = kwargs["logical_date"].date()
        print(f"Loading transactions for logical date: {logical_date}")

        folder = Path("/opt/airflow/data/batch/transactions")
        file_path = folder / f"transactions_{logical_date}.csv"
        print(f"Loading transactions from file: {file_path}")

        load_reference_csvs_to_minio(file_path, prefix="transactions")

    load_transactions()


transactions_ingestion_dag = transactions_ingestion()
