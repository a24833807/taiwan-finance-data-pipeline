from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="branching_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def branching_demo():

    @task.branch
    def check_data() -> str:
        has_data = False  # Simulate a condition to check for data existence

        if has_data:
            return "process_data"

        return "skip_process"

    @task
    def process_data():
        print("Data exists. Processing data.")

    @task
    def skip_process():
        print("No data. Skip processing.")

    @task(trigger_rule="none_failed_min_one_success")
    def finish():
        print("Pipeline finished.")

    branch = check_data()

    process = process_data()
    skip = skip_process()

    branch >> [process, skip]

    [process, skip] >> finish()


branching_demo()