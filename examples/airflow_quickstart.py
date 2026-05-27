"""
KAGE x Airflow - Quickstart DAG

Drop this file into your Airflow `dags/` folder. Each task instance will emit
a KAGE task_run event (start + SUCCESS / FAILED). The DAG run emits a
job_run end event so all tasks correlate by Airflow's `run_id`.

Tested patterns:
    1. DAG-wide default layer via `kage_task_callbacks` in default_args.
    2. Per-task layer override by unpacking callbacks into the operator.
    3. Inline dataset_event from inside the task body via
       `log_dataset_from_context`.

Install KAGE on the Airflow workers (pip install kage) and configure once at
module import.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context  # noqa: F401  (for type hints if you like)

from kage import configure
from kage.integrations.airflow import (
    kage_dag_callbacks,
    kage_task_callbacks,
    log_dataset_from_context,
)

# ---------- 1. Configure KAGE once -----------------------------------------
configure(
    base_path="/opt/airflow/kage-logs",
    pipeline_name="orders_etl",
    environment="prod",
    platform="airflow",
)


# ---------- 2. Task callables ----------------------------------------------
def extract(**context):
    # In real code: fetch from API / S3 / DB, write to a staging table.
    return 150_000


def clean_orders(**context):
    raw_count = context["ti"].xcom_pull(task_ids="extract")
    cleaned = int(raw_count * 0.85)
    # Emit a dataset_event from inside the task body — correlates with
    # the surrounding task_run via airflow run_id.
    log_dataset_from_context(
        context,
        layer="bronze",
        dataset_name="bronze_orders",
        record_count=cleaned,
        upstream_datasets=["raw_orders_api"],
    )
    return cleaned


def aggregate(**context):
    bronze_count = context["ti"].xcom_pull(task_ids="clean_orders")
    summary = bronze_count // 1500
    log_dataset_from_context(
        context,
        layer="gold",
        dataset_name="customer_summary",
        record_count=summary,
        upstream_datasets=["bronze_orders"],
    )
    return summary


# ---------- 3. Build the DAG -----------------------------------------------
with DAG(
    dag_id="orders_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Daily orders ETL with KAGE observability",
    # Apply task-level KAGE callbacks to every task by default (layer=silver).
    default_args={
        "owner": "data-platform",
        **kage_task_callbacks(layer="silver"),
    },
    # DAG-level callbacks emit job_run end events.
    **kage_dag_callbacks(),
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
        # Override the layer for this task — landing because we're reading raw.
        **kage_task_callbacks(layer="landing"),
    )

    clean_task = PythonOperator(
        task_id="clean_orders",
        python_callable=clean_orders,
        **kage_task_callbacks(layer="bronze"),
    )

    aggregate_task = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate,
        **kage_task_callbacks(layer="gold"),
    )

    extract_task >> clean_task >> aggregate_task
