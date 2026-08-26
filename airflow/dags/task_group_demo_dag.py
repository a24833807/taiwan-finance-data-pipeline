from datetime import datetime

from airflow.sdk import dag, task, task_group


@dag(
    dag_id="task_group_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def task_group_demo():

    @task
    def prepare():
        print("Prepare data.")

    @task_group(group_id="processing_group")
    def processing_group():

        @task
        def transform():
            print("Transform data.")

        @task
        def validate():
            print("Validate data.")

        transform_task = transform()
        validate_task = validate()

        transform_task >> validate_task

    @task
    def finish():
        print("Pipeline finished.")

    prepare_task = prepare()
    processing = processing_group()
    finish_task = finish()

    prepare_task >> processing >> finish_task


task_group_demo()