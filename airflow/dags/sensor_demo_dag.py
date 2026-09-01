from datetime import datetime

from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue


@dag(
    dag_id="sensor_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def sensor_demo():

    @task.sensor(
        poke_interval=5,
        timeout=20,
    )
    def wait_for_condition():

        print("Checking condition...")

        return PokeReturnValue(
            is_done=False
        )

    @task
    def process_data():
        print("Condition ready. Processing data.")

    wait = wait_for_condition()
    process = process_data()

    wait >> process


sensor_demo()