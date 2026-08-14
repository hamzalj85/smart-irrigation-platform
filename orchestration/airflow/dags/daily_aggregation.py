"""Daily aggregation: run dbt, then validate what it produced.

A dbt run that succeeds is not proof that the data is correct: dbt reports
success as long as the SQL executes. This DAG therefore separates *building*
from *verifying*. The second task fails the run if the marts are empty, stale,
or internally inconsistent -- so a silent upstream outage surfaces here instead
of in a dashboard three days later.
"""
from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DUCKDB_PATH = f"{DBT_DIR}/irrigation.duckdb"

# 26 h and not 24 h: the DAG runs at 02:00 on data of the previous day, so a
# lag slightly above one day is normal. Below this, an alert would be noise.
MAX_LAG_HOURS = 26


@dag(
    dag_id="daily_aggregation",
    description="dbt build, then row-count and freshness validation of the marts",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["dbt", "batch"],
    doc_md=__doc__,
)
def daily_aggregation():

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --no-use-colors",
        append_env=True,
    )

    @task
    def validate_marts() -> dict:
        """Four assertions on what dbt just wrote."""
        import duckdb
        from airflow.exceptions import AirflowFailException

        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        try:
            facts = con.execute(
                "select count(*) from fct_readings_hourly").fetchone()[0]
            devices = con.execute(
                "select count(*) from dim_devices").fetchone()[0]
            empty_buckets = con.execute(
                "select count(*) from fct_readings_hourly where reading_count < 1"
            ).fetchone()[0]
            lag_hours = con.execute(
                "select date_diff('hour', max(hour_start), current_timestamp) "
                "from fct_readings_hourly"
            ).fetchone()[0]
        finally:
            con.close()

        problems = []
        if facts == 0:
            problems.append("fct_readings_hourly is empty")
        if devices == 0:
            problems.append("dim_devices is empty")
        if empty_buckets:
            problems.append(f"{empty_buckets} hourly buckets with reading_count < 1")
        if lag_hours is not None and lag_hours > MAX_LAG_HOURS:
            problems.append(
                f"most recent hour is {lag_hours}h old (limit {MAX_LAG_HOURS}h)")

        if problems:
            raise AirflowFailException(
                "Mart validation failed: " + "; ".join(problems))

        return {"fact_rows": facts, "devices": devices, "lag_hours": lag_hours}

    dbt_build >> validate_marts()


daily_aggregation()
