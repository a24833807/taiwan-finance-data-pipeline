import time
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="pool_demo",
    start_date=datetime(2026, 9, 9),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def pool_demo():
    @task(pool="learning_pool")
    def task_a():
        print("Task A started")
        time.sleep(10)
        print("Task A finished")

    @task(pool="learning_pool")
    def task_b():
        print("Task B started")
        time.sleep(10)
        print("Task B finished")

    @task(pool="learning_pool")
    def task_c():
        print("Task C started")
        time.sleep(10)
        print("Task C finished")

    task_a()
    task_b()
    task_c()


pool_demo()
