from datetime import datetime

from airflow.sdk import DAG, get_current_context, task


with DAG(
    dag_id="twse_daily_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["twse", "etl"],
) as dag:

    @task(task_id="run_twse_pipeline")
    def run_twse_pipeline() -> int:
        context = get_current_context()
        dag_run = context["dag_run"]
        trade_date = (dag_run.conf or {}).get("trade_date")

        if not isinstance(trade_date, str):
            raise ValueError("dag_run.conf.trade_date must be a YYYYMMDD string")

        try:
            datetime.strptime(trade_date, "%Y%m%d")
        except ValueError as error:
            raise ValueError(
                "dag_run.conf.trade_date must be a valid date in YYYYMMDD format"
            ) from error

        from src.main import run_pipeline

        return run_pipeline(trade_date)

    run_twse_pipeline()
