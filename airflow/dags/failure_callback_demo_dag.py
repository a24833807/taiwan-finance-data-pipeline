from datetime import datetime, timedelta

from airflow.sdk import dag, task


def task_failure_callback(context):
    """Log basic information when a task reaches failure."""

    task_instance = context["task_instance"]
    exception = context.get("exception")

    print("=== FAILURE CALLBACK ===")
    print(f"DAG ID: {task_instance.dag_id}")
    print(f"Task ID: {task_instance.task_id}")
    print(f"Run ID: {task_instance.run_id}")
    print(f"Exception: {exception}")


@dag(
    dag_id="failure_callback_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def failure_callback_demo():

    @task
    def start():
        print("Pipeline started.")

    @task(
        retries=2,
        retry_delay=timedelta(seconds=3),
        on_failure_callback=task_failure_callback,
    )
    def failing_task():
        print("Processing data...")

        raise ValueError("Demo task failure")

    start_task = start()
    failed_task = failing_task()

    start_task >> failed_task


failure_callback_demo()