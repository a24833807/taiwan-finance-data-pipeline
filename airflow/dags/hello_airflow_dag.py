from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    @task
    def print_hello() -> None:
        print("Hello Airflow")
        print("Taiwan Finance Data Pipeline")

    print_hello()