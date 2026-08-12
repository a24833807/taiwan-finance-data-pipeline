from datetime import datetime

from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import Variable

@dag(
    dag_id="variable_connection_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def variable_connection_demo():

    @task
    def show_variable():
        environment = Variable.get("pipeline_environment")

        print(f"Pipeline environment: {environment}")

    @task
    def show_connection():
        connection = BaseHook.get_connection(
            "taiwan_finance_postgres"
        )

        print(f"Connection host: {connection.host}")
        print(f"Connection port: {connection.port}")
        print(f"Connection database: {connection.schema}")

    variable_task = show_variable()
    connection_task = show_connection()

    variable_task >> connection_task


variable_connection_demo()