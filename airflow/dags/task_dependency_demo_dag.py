from airflow.sdk import DAG, Param, get_current_context, task


with DAG(
    dag_id="task_dependency_demo",
    schedule=None,
    catchup=False,
    params={
        "force_failure": Param(
            default=False,
            type="boolean",
            description="Raise an intentional error in process for dependency testing",
        )
    },
    tags=["learning", "dependency"],
) as dag:

    @task
    def prepare() -> None:
        print("Preparing dependency demo")

    @task
    def process() -> None:
        context = get_current_context()
        force_failure = context["params"]["force_failure"]

        if force_failure:
            raise RuntimeError("Intentional failure for dependency testing")

        print("Process completed successfully")

    @task
    def finish() -> None:
        print("Dependency demo finished")

    prepare_task = prepare()
    process_task = process()
    finish_task = finish()

    prepare_task >> process_task >> finish_task
