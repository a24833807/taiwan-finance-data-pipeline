from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="dynamic_task_mapping_demo",
    start_date=datetime(2026, 9, 15),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def dynamic_task_mapping_demo():

    @task
    def get_trade_dates() -> list[str]:
        return [
            "20260901",
            "20260902",
            "20260903",
        ]

    @task
    def process_date(trade_date: str) -> None:
        print(f"Processing: {trade_date}")
        if trade_date == "20260902":
            raise ValueError("Day 31 mapping failure demo")

    trade_dates = get_trade_dates()

    process_date.expand(
        trade_date=trade_dates
    )


dynamic_task_mapping_demo()