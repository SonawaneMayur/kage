"""
KAGE Declarative API - @ decorators for zero-boilerplate observability
KAGE-PROPRIETARY-2026-v1.1

    @pipeline("daily_etl")
    def run_etl():
        bronze_df = clean_orders(raw_df)
        return aggregate(bronze_df)

    @task(layer="bronze", task_name="clean_orders")
    def clean_orders(df):
        return df.filter(df.amount > 0)

    @dataset(layer="gold", dataset_name="customer_summary")
    def aggregate(df):
        return df.groupBy("customer_id").count()
"""
import functools
import traceback
from threading import RLock
from typing import Any, Callable, List, Optional

from .core import KageLogger

_lock = RLock()
_default_logger: Optional[KageLogger] = None


def configure(**kwargs) -> KageLogger:
    """Set the default KageLogger used by the @ decorators.

    Any kwarg accepted by KageLogger (base_path, pipeline_name, environment, ...)
    can be passed here.
    """
    global _default_logger
    with _lock:
        _default_logger = KageLogger(**kwargs)
    return _default_logger


def set_default_logger(logger: KageLogger) -> None:
    global _default_logger
    with _lock:
        _default_logger = logger


def get_default_logger() -> KageLogger:
    global _default_logger
    with _lock:
        if _default_logger is None:
            _default_logger = KageLogger()
        return _default_logger


def _error_fields(exc: BaseException) -> dict:
    return {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "stack_trace": traceback.format_exc(),
    }


def pipeline(job_name=None, *, logger: Optional[KageLogger] = None, **fields) -> Callable:
    """Wrap a function as a KAGE job: emits job_start before, job_end after.

    On exception, emits status=FAILED with error_type/message/stack_trace,
    then re-raises so the caller still sees the error.

    Usage:
        @pipeline                       # uses function name as job_name
        @pipeline("daily_etl")          # explicit job_name
        @pipeline("daily_etl", owner="data-team")  # extra custom_fields
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            lg = logger or get_default_logger()
            name = job_name if isinstance(job_name, str) else func.__name__
            job_id = lg.job_start(name, **fields)
            try:
                result = func(*args, **kwargs)
                lg.job_end(job_id, "SUCCESS")
                return result
            except Exception as exc:
                lg.job_end(job_id, "FAILED", **_error_fields(exc))
                raise
        return wrapper

    if callable(job_name):
        func, job_name = job_name, None
        return decorator(func)
    return decorator


def task(layer: str, task_name: Optional[str] = None, *,
         logger: Optional[KageLogger] = None, **fields) -> Callable:
    """Wrap a function as a KAGE task in a medallion layer.

    Emits task_start before invocation and task_end (SUCCESS or FAILED) after.
    Re-raises any exception.

    Usage:
        @task(layer="bronze")
        def clean_orders(df): ...

        @task(layer="silver", task_name="dedupe")
        def remove_duplicates(df): ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            lg = logger or get_default_logger()
            name = task_name or func.__name__
            task_id = lg.task_start(layer, name, **fields)
            try:
                result = func(*args, **kwargs)
                lg.task_end(task_id, "SUCCESS")
                return result
            except Exception as exc:
                lg.task_end(task_id, "FAILED", **_error_fields(exc))
                raise
        return wrapper
    return decorator


def _infer_record_count(value: Any) -> int:
    """Best-effort record count from common return types.

    PySpark DataFrame -> .count(); int -> itself; sized container -> len().
    Streaming DataFrames are skipped (count() would block).
    Returns 0 if nothing applies.
    """
    if value is None:
        return 0
    if getattr(value, "isStreaming", False):
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    counter = getattr(value, "count", None)
    if callable(counter):
        try:
            n = counter()
            if isinstance(n, int):
                return n
        except Exception:
            pass
    try:
        return len(value)
    except TypeError:
        return 0


def dataset(layer: str, dataset_name: Optional[str] = None, *,
            action: str = "WRITE",
            upstream_datasets: Optional[List[str]] = None,
            record_count_fn: Optional[Callable[[Any], int]] = None,
            logger: Optional[KageLogger] = None,
            **fields) -> Callable:
    """Wrap a function that produces (WRITE) or consumes (READ) a dataset.

    The decorator calls the function, then logs a dataset_event with a
    record count derived from the return value.

    record_count_fn: optional custom extractor; defaults to PySpark .count(),
    int returns, or len() — whichever fits.

    Usage:
        @dataset(layer="gold", dataset_name="customer_summary",
                 upstream_datasets=["bronze.orders"])
        def build_summary(df):
            return df.groupBy("customer_id").count()
    """
    if action.upper() not in ("READ", "WRITE"):
        raise ValueError(f"action must be READ or WRITE, got {action!r}")

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            lg = logger or get_default_logger()
            name = dataset_name or func.__name__
            result = func(*args, **kwargs)

            if record_count_fn is not None:
                try:
                    count = int(record_count_fn(result))
                except Exception:
                    count = 0
            else:
                count = _infer_record_count(result)

            emit = lg.dataset_write if action.upper() == "WRITE" else lg.dataset_read
            emit(layer, name, record_count=count,
                 upstream_datasets=upstream_datasets, **fields)
            return result
        return wrapper
    return decorator


kage_pipeline = pipeline
kage_task = task
kage_dataset = dataset
