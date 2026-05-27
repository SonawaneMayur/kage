"""
KAGE x Airflow - Positive / negative / extreme case handling

You DON'T need Airflow installed to read this file - the patterns below are
copy-paste ready into a real DAG. The bottom of the file is a self-contained
mini-runner that fabricates Airflow-shaped contexts and exercises the
callbacks so you can see KAGE event output locally.

Patterns:
    A. Positive   - 3-task DAG with KAGE callbacks via default_args
    B. Per-task   - override the layer for a specific task
    C. Negative   - task raises -> KAGE logs FAILED + stack_trace, re-raises
    D. DAG-level  - DAG run end emits job_run SUCCESS / FAILED
    E. Inline     - log dataset_events from inside a task body
"""
import json
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace

from kage import KageLogger
from kage.integrations.airflow import (
    kage_dag_callbacks,
    kage_task_callbacks,
    log_dataset_from_context,
)


# --------------------------------------------------------------------------- #
# Real-world snippets (do not execute here; this is documentation)

DAG_SNIPPET = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from kage import configure
from kage.integrations.airflow import kage_dag_callbacks, kage_task_callbacks

configure(base_path="/opt/airflow/kage-logs",
          pipeline_name="orders_etl", platform="airflow")

with DAG(
    dag_id="orders_etl",
    default_args={**kage_task_callbacks(layer="silver")},
    **kage_dag_callbacks(),
    ...
) as dag:
    ingest = PythonOperator(
        task_id="ingest", python_callable=ingest_fn,
        **kage_task_callbacks(layer="bronze"),   # per-task override
    )
'''


# --------------------------------------------------------------------------- #
# Local mini-runner: fabricate Airflow contexts so we can see real KAGE events

def make_ctx(dag_id, run_id, task_id, *, exception=None):
    ti = SimpleNamespace(task_id=task_id, dag_id=dag_id,
                         try_number=1, duration=0.42)
    return {
        "task_instance": ti,
        "ti": ti,
        "dag": SimpleNamespace(dag_id=dag_id),
        "dag_run": SimpleNamespace(run_id=run_id),
        "run_id": run_id,
        "exception": exception,
    }


def _count_files(base, ev_type):
    return len(list(Path(base).glob(f"**/event_type={ev_type}/**/part-*.jsonl")))


def case_positive():
    """A + B + E. 3 tasks, mixed layers, with inline dataset events."""
    base = tempfile.mkdtemp(prefix="kage-af-pos-")
    lg = KageLogger(base_path=base, pipeline_name="orders_etl",
                    platform="airflow")
    bronze = kage_task_callbacks(layer="bronze", logger=lg)
    silver = kage_task_callbacks(layer="silver", logger=lg)
    gold = kage_task_callbacks(layer="gold", logger=lg)
    dag_cb = kage_dag_callbacks(logger=lg)

    run_id = "manual__2026-05-26T00:00:00"
    for cb, task_id, layer in (
        (bronze, "ingest", "bronze"),
        (silver, "clean", "silver"),
        (gold,   "rollup", "gold"),
    ):
        ctx = make_ctx("orders_etl", run_id, task_id)
        cb["on_execute_callback"](ctx)
        # Pretend the task body emits a dataset_event:
        log_dataset_from_context(ctx, layer=layer,
                                 dataset_name=f"{task_id}_out",
                                 record_count=10_000, logger=lg)
        cb["on_success_callback"](ctx)

    dag_cb["on_success_callback"](make_ctx("orders_etl", run_id, "_dag"))

    print("A+B+E POSITIVE")
    for ev in ("job_run", "task_run", "dataset_event"):
        print(f"  {ev}: {_count_files(base, ev)} files")
    shutil.rmtree(base, ignore_errors=True)


def case_negative_task_failure():
    """C. one task raises; KAGE logs FAILED, re-raise is the user's job."""
    base = tempfile.mkdtemp(prefix="kage-af-neg-")
    lg = KageLogger(base_path=base, platform="airflow")
    cbs = kage_task_callbacks(layer="silver", logger=lg)
    run_id = "manual__neg"

    ctx = make_ctx("orders_etl", run_id, "transform")
    cbs["on_execute_callback"](ctx)
    try:
        raise ValueError("downstream timeout")
    except ValueError as exc:
        ctx["exception"] = exc
        cbs["on_failure_callback"](ctx)

    failed = []
    for f in Path(base).glob("**/event_type=task_run/**/part-*.jsonl"):
        for line in f.read_text().splitlines():
            ev = json.loads(line)
            if ev.get("status") == "FAILED":
                failed.append(ev)

    print("\nC NEGATIVE: task failure")
    print(f"  FAILED task_run events: {len(failed)}")
    if failed:
        cf = failed[0]["custom_fields"]
        print(f"  error_type:    {cf['error_type']}")
        print(f"  error_message: {cf['error_message']}")
        print(f"  stack_trace:   {'yes' if cf.get('stack_trace') else 'no'}")
    shutil.rmtree(base, ignore_errors=True)


def case_dag_failure():
    """D. DAG run fails; on_failure_callback emits FAILED job_run."""
    base = tempfile.mkdtemp(prefix="kage-af-dag-")
    lg = KageLogger(base_path=base, platform="airflow")
    dag_cb = kage_dag_callbacks(logger=lg)
    run_id = "manual__dag_fail"
    try:
        raise RuntimeError("dag-level failure")
    except RuntimeError as exc:
        ctx = make_ctx("orders_etl", run_id, "_dag", exception=exc)
        dag_cb["on_failure_callback"](ctx)
    print("\nD DAG FAILURE")
    print(f"  job_run files: {_count_files(base, 'job_run')}")
    shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    print("KAGE x Airflow - Case Handling Demo")
    print("DAG snippet (paste into your dags/ folder):")
    print(DAG_SNIPPET)
    case_positive()
    case_negative_task_failure()
    case_dag_failure()
