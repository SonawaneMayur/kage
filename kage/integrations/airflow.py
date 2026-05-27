"""
KAGE x Airflow integration
==========================

Callback factories you wire into your Airflow DAG so every TaskInstance emits
a KAGE task_run event (start + end) and every DAG run emits a job_run end.

    from airflow import DAG
    from kage import configure
    from kage.integrations.airflow import (
        kage_task_callbacks, kage_dag_callbacks,
    )

    configure(base_path="/opt/airflow/kage-logs",
              pipeline_name="orders_etl", platform="airflow")

    with DAG(
        dag_id="orders_etl",
        default_args={**kage_task_callbacks(layer="bronze")},
        **kage_dag_callbacks(),
        ...,
    ) as dag:
        ...

Per-task layer overrides:
    bronze_task = PythonOperator(
        task_id="bronze_clean",
        python_callable=clean,
        **kage_task_callbacks(layer="bronze"),
    )

Notes:
- Every Airflow task instance becomes one KAGE task_run. The KAGE
  job_run_id is set to the Airflow `run_id`, so all tasks of a DAG run
  share one job.
- KAGE never swallows exceptions - the failure callback emits FAILED with
  full stack trace; Airflow's normal retry / failure handling proceeds.
- No hard dependency on the `airflow` package: callbacks accept a plain
  context dict (which Airflow provides), so this module imports cleanly
  outside Airflow workers too.
"""
from __future__ import annotations

import traceback
from typing import Any, Callable, Dict, Optional, Tuple

from ..core import KageLogger
from ..decorators import get_default_logger


def _get_logger(logger: Optional[KageLogger]) -> KageLogger:
    return logger if logger is not None else get_default_logger()


def _context_run_id(context: Dict[str, Any]) -> Optional[str]:
    dag_run = context.get("dag_run")
    if dag_run is not None:
        rid = getattr(dag_run, "run_id", None)
        if rid:
            return rid
    return context.get("run_id")


def _context_task_info(context: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Any]:
    ti = context.get("task_instance") or context.get("ti")
    task_id = None
    dag_id = None
    if ti is not None:
        task_id = getattr(ti, "task_id", None)
        dag_id = getattr(ti, "dag_id", None)
    if dag_id is None:
        dag = context.get("dag")
        if dag is not None:
            dag_id = getattr(dag, "dag_id", None)
    if task_id is None:
        task = context.get("task")
        if task is not None:
            task_id = getattr(task, "task_id", None)
    return task_id, dag_id, ti


def _format_exception(exc: Optional[BaseException]) -> Dict[str, Any]:
    if exc is None:
        return {"error_type": "Unknown", "error_message": "", "stack_trace": ""}
    if exc.__traceback__ is not None:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    else:
        tb = "".join(traceback.format_exception_only(type(exc), exc))
    return {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "stack_trace": tb,
    }


def kage_task_callbacks(
    layer: str = "silver",
    *,
    logger: Optional[KageLogger] = None,
) -> Dict[str, Callable[[Dict[str, Any]], None]]:
    """Return a dict of Airflow task callbacks tied to one medallion `layer`.

    Use in `default_args` (DAG-wide) or unpack into individual operators
    for per-task layer overrides.

    Returned keys:
        on_execute_callback   -> KAGE task_start
        on_success_callback   -> KAGE task_end (SUCCESS)
        on_failure_callback   -> KAGE task_end (FAILED, with stack_trace)
    """

    def on_execute(context: Dict[str, Any]) -> None:
        lg = _get_logger(logger)
        task_id, dag_id, ti = _context_task_info(context)
        run_id = _context_run_id(context)
        # Make all tasks of this DAG run share one job_run_id.
        if run_id:
            lg.active_runs["job"] = run_id
        kage_task_id = lg.task_start(
            layer=layer,
            task_name=task_id or "unnamed_task",
            airflow_dag_id=dag_id,
            airflow_run_id=run_id,
            airflow_try_number=getattr(ti, "try_number", None),
        )
        if ti is not None:
            try:
                ti.kage_task_id = kage_task_id  # type: ignore[attr-defined]
            except Exception:
                pass

    def on_success(context: Dict[str, Any]) -> None:
        lg = _get_logger(logger)
        task_id, _dag_id, ti = _context_task_info(context)
        run_id = _context_run_id(context)
        kage_task_id = getattr(ti, "kage_task_id", None) if ti is not None else None
        if not kage_task_id:
            kage_task_id = f"{run_id or 'unknown'}-{task_id or 'unnamed'}"
        lg.task_end(
            kage_task_id,
            "SUCCESS",
            airflow_state="success",
            duration_sec=getattr(ti, "duration", None),
        )

    def on_failure(context: Dict[str, Any]) -> None:
        lg = _get_logger(logger)
        task_id, _dag_id, ti = _context_task_info(context)
        run_id = _context_run_id(context)
        kage_task_id = getattr(ti, "kage_task_id", None) if ti is not None else None
        if not kage_task_id:
            kage_task_id = f"{run_id or 'unknown'}-{task_id or 'unnamed'}"
        exc = context.get("exception")
        lg.task_end(
            kage_task_id,
            "FAILED",
            airflow_state="failed",
            duration_sec=getattr(ti, "duration", None),
            **_format_exception(exc),
        )

    return {
        "on_execute_callback": on_execute,
        "on_success_callback": on_success,
        "on_failure_callback": on_failure,
    }


def kage_dag_callbacks(
    *,
    logger: Optional[KageLogger] = None,
) -> Dict[str, Callable[[Dict[str, Any]], None]]:
    """Return DAG-level callbacks (pass as kwargs to `DAG(...)`).

    Returned keys:
        on_success_callback -> KAGE job_end (SUCCESS)
        on_failure_callback -> KAGE job_end (FAILED, with stack_trace)

    The KAGE job_run_id matches Airflow's run_id, so it correlates 1:1 with
    the task_run events emitted by `kage_task_callbacks`.
    """

    def on_success(context: Dict[str, Any]) -> None:
        lg = _get_logger(logger)
        run_id = _context_run_id(context)
        dag = context.get("dag")
        lg.job_end(
            run_id or "unknown",
            "SUCCESS",
            airflow_dag_id=getattr(dag, "dag_id", None),
        )

    def on_failure(context: Dict[str, Any]) -> None:
        lg = _get_logger(logger)
        run_id = _context_run_id(context)
        dag = context.get("dag")
        exc = context.get("exception")
        lg.job_end(
            run_id or "unknown",
            "FAILED",
            airflow_dag_id=getattr(dag, "dag_id", None),
            **_format_exception(exc),
        )

    return {
        "on_success_callback": on_success,
        "on_failure_callback": on_failure,
    }


def log_dataset_from_context(
    context: Dict[str, Any],
    *,
    layer: str,
    dataset_name: str,
    record_count: int = 0,
    upstream_datasets: Optional[list] = None,
    action: str = "WRITE",
    logger: Optional[KageLogger] = None,
    **fields: Any,
) -> str:
    """Emit a KAGE dataset_event from within an Airflow task body.

    Pulls the airflow run_id from context so the dataset_event correlates
    with the job_run / task_run emitted by callbacks.
    """
    lg = _get_logger(logger)
    run_id = _context_run_id(context)
    if run_id:
        lg.active_runs["job"] = run_id
    task_id, dag_id, _ti = _context_task_info(context)
    fields.setdefault("airflow_dag_id", dag_id)
    fields.setdefault("airflow_run_id", run_id)
    fields.setdefault("airflow_task_id", task_id)
    emit = lg.dataset_write if action.upper() == "WRITE" else lg.dataset_read
    return emit(
        layer=layer,
        dataset_name=dataset_name,
        record_count=record_count,
        upstream_datasets=upstream_datasets,
        **fields,
    )
