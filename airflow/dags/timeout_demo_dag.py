import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="timeout_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def timeout_demo():

    @task(
        execution_timeout=timedelta(seconds=10)
    )
    def quick_task():
        print("Quick task started.")

        time.sleep(2)

        print("Quick task finished.")

    @task(
        retries=2,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=20)
    )
    def slow_task():
        print("Slow task started.")

        time.sleep(30)

        print("Slow task finished.")

    quick = quick_task()
    slow = slow_task()

    quick >> slow


timeout_demo()