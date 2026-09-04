from datetime import datetime

from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue


@dag(
    dag_id="sensor_mode_demo",
    start_date=datetime(2026, 9, 4),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def sensor_mode_demo():
    @task.sensor(
        task_id="poke_sensor",
        mode="poke",
        poke_interval=5,
        timeout=30,
    )
    def poke_sensor():
        print("poke mode: condition is not ready yet.")
        return PokeReturnValue(is_done=False)

    @task.sensor(
        task_id="reschedule_sensor",
        mode="reschedule",
        poke_interval=5,
        timeout=30,
    )
    def reschedule_sensor():
        print("reschedule mode: condition is not ready yet.")
        return PokeReturnValue(is_done=False)

    poke_sensor()
    reschedule_sensor()


sensor_mode_demo()
