"""
KAGE x Spark Declarative Pipelines (Delta Live Tables / Lakeflow Declarative Pipelines)
KAGE-PROPRIETARY-2026-v1.1

Stacks KAGE observability on top of Databricks' declarative pipeline decorators
(`@dlt.table`, `@dlt.view`, `@dlt.expect_*`). Each materialization emits a
KAGE `dataset_event` with lineage, layer, and (optionally) record count.

Quick reference
---------------
    from kage.integrations.spark_declarative import (
        kage_dlt_table, kage_dlt_view, kage_dlt_expectations,
    )

    @kage_dlt_table(layer="bronze", upstream_datasets=["landing.orders"])
    def bronze_orders():
        return spark.readStream.format("cloudFiles").load("/landing/orders")

    @kage_dlt_table(
        layer="silver",
        upstream_datasets=["bronze_orders"],
        comment="Validated orders",
    )
    @kage_dlt_expectations(
        ("expect_or_drop", "valid_amount", "amount > 0"),
        ("expect_or_fail", "non_null_id", "order_id IS NOT NULL"),
    )
    def silver_orders():
        return dlt.read_stream("bronze_orders")

Why a wrapper?
--------------
DLT materializes the function for you, so plain `@kage.dataset` would either:
  (a) miss the run entirely (DLT calls the function inside its own runner), or
  (b) trigger an extra `df.count()` action - doubling compute.

`kage_dlt_table` solves both: it lets DLT call the function, logs a KAGE
event each call, and skips `count()` by default (toggle with `skip_count=False`
or pass a custom `record_count_fn`).
"""
import functools
import traceback
from typing import Callable, List, Optional, Tuple

from ..core import KageLogger
from ..decorators import get_default_logger

try:
    import dlt as _dlt
    _HAS_DLT = True
except ImportError:
    _dlt = None
    _HAS_DLT = False


_VALID_EXPECTATIONS = {
    "expect", "expect_or_drop", "expect_or_fail",
    "expect_all", "expect_all_or_drop", "expect_all_or_fail",
}


def _require_dlt():
    if not _HAS_DLT:
        raise ImportError(
            "Spark Declarative Pipelines (dlt) module not available. "
            "This integration must run inside a Databricks DLT / Lakeflow "
            "Declarative Pipelines job."
        )


def _kage_wrap(
    func: Callable,
    *,
    layer: str,
    dataset_name: str,
    action: str,
    upstream_datasets: Optional[List[str]],
    record_count_fn: Optional[Callable],
    skip_count: bool,
    logger: Optional[KageLogger],
    extra_fields: dict,
) -> Callable:
    """Inner wrapper: runs func, emits KAGE dataset_event, re-raises on error."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        lg = logger or get_default_logger()
        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            lg._dataset_event(
                action=action,
                layer=layer,
                dataset_name=dataset_name,
                record_count=0,
                upstream_datasets=upstream_datasets,
                status="FAILED",
                error_type=type(exc).__name__,
                error_message=str(exc),
                stack_trace=traceback.format_exc(),
                **extra_fields,
            )
            raise

        if record_count_fn is not None:
            try:
                count = int(record_count_fn(result))
            except Exception:
                count = 0
        elif skip_count:
            count = 0
        else:
            from ..decorators import _infer_record_count
            count = _infer_record_count(result)

        emit = lg.dataset_write if action == "WRITE" else lg.dataset_read
        emit(
            layer=layer,
            dataset_name=dataset_name,
            record_count=count,
            upstream_datasets=upstream_datasets,
            status="SUCCESS",
            **extra_fields,
        )
        return result

    return wrapper


def kage_dlt_table(
    name: Optional[str] = None,
    *,
    layer: str,
    upstream_datasets: Optional[List[str]] = None,
    comment: Optional[str] = None,
    skip_count: bool = True,
    record_count_fn: Optional[Callable] = None,
    logger: Optional[KageLogger] = None,
    extra_fields: Optional[dict] = None,
    **dlt_kwargs,
) -> Callable:
    """`@dlt.table` plus KAGE dataset_event logging.

    Args:
        name: Table name (defaults to function name).
        layer: Medallion layer (landing/bronze/silver/gold). Required.
        upstream_datasets: List of upstream dataset identifiers - logged as lineage.
        comment: Forwarded to `dlt.table(comment=...)`.
        skip_count: If True (default), do not call `.count()` on the result.
            Streaming DataFrames are always skipped regardless.
        record_count_fn: Custom record-count extractor, e.g. `lambda df: df.count()`.
        logger: Optional KageLogger override (otherwise uses `get_default_logger()`).
        extra_fields: Extra custom_fields merged into the KAGE event.
        **dlt_kwargs: Forwarded to `dlt.table` (e.g. `table_properties`, `partition_cols`).

    Stack order (innermost to outermost):
        function -> KAGE wrapper -> @dlt.table
    """
    _require_dlt()

    def decorator(func: Callable) -> Callable:
        ds_name = name or func.__name__
        kage_wrapped = _kage_wrap(
            func,
            layer=layer,
            dataset_name=ds_name,
            action="WRITE",
            upstream_datasets=upstream_datasets,
            record_count_fn=record_count_fn,
            skip_count=skip_count,
            logger=logger,
            extra_fields=extra_fields or {},
        )
        return _dlt.table(name=name, comment=comment, **dlt_kwargs)(kage_wrapped)

    return decorator


def kage_dlt_view(
    name: Optional[str] = None,
    *,
    layer: str = "silver",
    upstream_datasets: Optional[List[str]] = None,
    comment: Optional[str] = None,
    logger: Optional[KageLogger] = None,
    extra_fields: Optional[dict] = None,
    **dlt_kwargs,
) -> Callable:
    """`@dlt.view` plus KAGE dataset_event (action=READ, lazy materialization)."""
    _require_dlt()

    def decorator(func: Callable) -> Callable:
        ds_name = name or func.__name__
        kage_wrapped = _kage_wrap(
            func,
            layer=layer,
            dataset_name=ds_name,
            action="READ",
            upstream_datasets=upstream_datasets,
            record_count_fn=None,
            skip_count=True,  # views are virtual - never count
            logger=logger,
            extra_fields=extra_fields or {},
        )
        return _dlt.view(name=name, comment=comment, **dlt_kwargs)(kage_wrapped)

    return decorator


def kage_dlt_expectations(*expectations: Tuple[str, str, str]) -> Callable:
    """Apply multiple `@dlt.expect*` decorators in one call.

    Each tuple is (action, name, constraint), where action is one of:
        expect, expect_or_drop, expect_or_fail,
        expect_all, expect_all_or_drop, expect_all_or_fail

    Example:
        @kage_dlt_table(layer="silver", upstream_datasets=["bronze_orders"])
        @kage_dlt_expectations(
            ("expect_or_drop", "valid_amount", "amount > 0"),
            ("expect_or_fail", "non_null_id", "order_id IS NOT NULL"),
        )
        def silver_orders():
            return dlt.read("bronze_orders")
    """
    _require_dlt()
    for action, _name, _constraint in expectations:
        if action not in _VALID_EXPECTATIONS:
            raise ValueError(
                f"Invalid expectation action {action!r}. "
                f"Must be one of: {sorted(_VALID_EXPECTATIONS)}"
            )

    def decorator(func: Callable) -> Callable:
        wrapped = func
        for action, name, constraint in reversed(expectations):
            dlt_decorator = getattr(_dlt, action)
            if action.startswith("expect_all"):
                wrapped = dlt_decorator({name: constraint})(wrapped)
            else:
                wrapped = dlt_decorator(name, constraint)(wrapped)
        return wrapped

    return decorator


def is_dlt_available() -> bool:
    """True when running inside a DLT/Lakeflow runtime (the `dlt` module imports)."""
    return _HAS_DLT
