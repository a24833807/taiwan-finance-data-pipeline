from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, get_current_context, task


with DAG(
    dag_id="twse_daily_pipeline",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    params={
        "trade_date": Param(
            default=None,
            type=["null", "string"],
            pattern=r"^\d{8}$",
            description="Optional trade date in YYYYMMDD format",
        )
    },
    tags=["twse", "etl"],
) as dag:

    @task(task_id="run_twse_pipeline")
    def run_twse_pipeline() -> int:
        context = get_current_context()
        manual_trade_date = context["params"].get("trade_date")

        if manual_trade_date:
            trade_date = manual_trade_date
            date_source = "manual parameter"
        else:
            data_interval_start = context["data_interval_start"]
            trade_date = data_interval_start.strftime("%Y%m%d")
            date_source = "data interval"

        print(f"Resolved trade_date={trade_date}, source={date_source}")

        if not isinstance(trade_date, str):
            raise ValueError("trade_date must be a YYYYMMDD string")

        try:
            datetime.strptime(trade_date, "%Y%m%d")
        except ValueError as error:
            raise ValueError(
                "trade_date must be a valid date in YYYYMMDD format"
            ) from error

        from src.main import run_pipeline

        return run_pipeline(trade_date)

    run_twse_pipeline()
