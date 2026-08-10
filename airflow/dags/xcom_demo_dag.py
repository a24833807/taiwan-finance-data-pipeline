from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="xcom_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "xcom"],
) as dag:

    @task
    def prepare() -> str:
        trade_date = "20260810"

        print(f"Prepared trade_date: {trade_date}")

        return trade_date

    @task
    def process(trade_date: str) -> dict:
        print(f"Received trade_date: {trade_date}")

        result = {
            "trade_date": trade_date,
            "row_count": 1000,
            "status": "success",
        }

        return result

    @task
    def finish(result: dict) -> None:
        print(f"Trade date: {result['trade_date']}")
        print(f"Row count: {result['row_count']}")
        print(f"Status: {result['status']}")

    trade_date = prepare()
    result = process(trade_date)
    finish(result)