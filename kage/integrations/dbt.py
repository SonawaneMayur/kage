"""
KAGE x dbt integration
=======================

After a `dbt run`, dbt writes JSON artifacts to `target/`:
  - run_results.json  -> status, timing, rows_affected per node
  - manifest.json     -> model metadata: tags, depends_on, schema, resource_type

`emit_dbt_run_results(target_dir, ...)` parses those artifacts and emits one
job_run + one task_run + one dataset_event per model (or snapshot / seed).

Programmatic:
    from kage.integrations.dbt import emit_dbt_run_results

    emit_dbt_run_results(
        target_dir="target/",
        pipeline_name="orders_dbt",
        base_path="./kage-logs",
        default_layer="silver",
    )

CLI:
    python -m kage.integrations.dbt target/ \\
        --pipeline-name orders_dbt --base-path ./kage-logs

Layer inference (medallion):
  1. Model has a tag matching landing / bronze / silver / gold (case-insensitive)
  2. else: any of those layer names appears in the model's schema name
  3. else: `default_layer` (default "silver")

Lineage:
  manifest.json's `depends_on.nodes` are resolved to upstream model names and
  emitted as `upstream_datasets` on the dataset_event.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..core import KageLogger

_DBT_LAYER_TAGS = {"landing", "bronze", "silver", "gold"}

_DBT_STATUS_MAP = {
    "success": "SUCCESS",
    "pass": "SUCCESS",
    "fail": "FAILED",
    "error": "FAILED",
    "skipped": "SKIPPED",
    "runtime error": "FAILED",
}


def _infer_layer(node: Optional[Dict[str, Any]], default_layer: str) -> str:
    """Extract medallion layer from a manifest node's tags / schema."""
    if not node:
        return default_layer
    tags: List[str] = list(node.get("tags") or [])
    cfg = node.get("config") or {}
    tags += list(cfg.get("tags") or [])
    for tag in tags:
        if isinstance(tag, str) and tag.lower() in _DBT_LAYER_TAGS:
            return tag.lower()
    schema = (node.get("schema") or "").lower()
    for layer in _DBT_LAYER_TAGS:
        if layer in schema:
            return layer
    return default_layer


def _node_upstream_datasets(
    node: Optional[Dict[str, Any]],
    manifest_nodes: Dict[str, Dict[str, Any]],
) -> List[str]:
    if not node:
        return []
    deps = (node.get("depends_on") or {}).get("nodes", [])
    out: List[str] = []
    for dep in deps:
        upstream = manifest_nodes.get(dep)
        if upstream:
            out.append(upstream.get("name") or upstream.get("alias") or dep.split(".")[-1])
        else:
            # Sources / unknown nodes: keep the unique_id tail
            out.append(dep.split(".")[-1])
    return out


def _rows_affected(result: Dict[str, Any]) -> int:
    adapter = result.get("adapter_response") or {}
    raw = adapter.get("rows_affected")
    try:
        return int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        return 0


def emit_dbt_run_results(
    target_dir,
    *,
    pipeline_name: str = "dbt_project",
    base_path: Optional[str] = None,
    logger: Optional[KageLogger] = None,
    default_layer: str = "silver",
    job_name: str = "dbt_run",
    environment: str = "dev",
) -> str:
    """Parse a dbt target/ directory and emit KAGE events.

    Returns the KAGE job_run_id for this dbt invocation.
    """
    target_path = Path(target_dir)
    run_results_path = target_path / "run_results.json"
    manifest_path = target_path / "manifest.json"

    if not run_results_path.exists():
        raise FileNotFoundError(
            f"run_results.json not found at {run_results_path}. "
            "Did you run `dbt run` (or test/build) yet?"
        )

    run_results = json.loads(run_results_path.read_text())
    manifest_nodes: Dict[str, Dict[str, Any]] = {}
    if manifest_path.exists():
        try:
            manifest_nodes = json.loads(manifest_path.read_text()).get("nodes", {}) or {}
        except json.JSONDecodeError:
            manifest_nodes = {}

    if logger is None:
        kwargs: Dict[str, Any] = {
            "pipeline_name": pipeline_name,
            "language": "sql",
            "platform": "dbt",
            "environment": environment,
        }
        if base_path is not None:
            kwargs["base_path"] = base_path
        logger = KageLogger(**kwargs)

    target_name = (run_results.get("args") or {}).get("target", "default")
    elapsed = run_results.get("elapsed_time")
    invocation_id = run_results.get("metadata", {}).get("invocation_id")

    job_id = logger.job_start(
        job_name,
        dbt_target=target_name,
        dbt_invocation_id=invocation_id,
        elapsed_time=elapsed,
    )

    overall_status = "SUCCESS"
    results = run_results.get("results", []) or []

    for result in results:
        unique_id = result.get("unique_id", "")
        node = manifest_nodes.get(unique_id, {})
        node_name = node.get("name") or unique_id.split(".")[-1]
        resource_type = node.get("resource_type") or "model"
        layer = _infer_layer(node, default_layer)
        upstream = _node_upstream_datasets(node, manifest_nodes)
        record_count = _rows_affected(result)
        status = _DBT_STATUS_MAP.get(
            str(result.get("status", "")).lower(), "SUCCESS"
        )
        if status == "FAILED":
            overall_status = "FAILED"

        task_id = logger.task_start(
            layer=layer,
            task_name=node_name,
            dbt_unique_id=unique_id,
            dbt_resource_type=resource_type,
        )
        logger.task_end(
            task_id,
            status,
            output_count=record_count if status == "SUCCESS" else None,
            error_message=result.get("message") if status == "FAILED" else None,
            execution_time_sec=result.get("execution_time"),
        )

        if resource_type in ("model", "snapshot", "seed") and status == "SUCCESS":
            logger.dataset_write(
                layer=layer,
                dataset_name=node_name,
                record_count=record_count,
                upstream_datasets=upstream,
                dbt_unique_id=unique_id,
                dbt_resource_type=resource_type,
            )

    logger.job_end(
        job_id,
        overall_status,
        model_count=len(results),
        elapsed_time=elapsed,
    )
    return job_id


def main(argv: Optional[List[str]] = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="kage.integrations.dbt",
        description="Ingest dbt target/ artifacts and emit KAGE events.",
    )
    parser.add_argument("target_dir", help="path to dbt target/ directory")
    parser.add_argument("--pipeline-name", default="dbt_project")
    parser.add_argument("--base-path", default=None,
                        help="KAGE log base path (defaults to /tmp/kage-logs)")
    parser.add_argument("--default-layer", default="silver",
                        choices=["landing", "bronze", "silver", "gold"])
    parser.add_argument("--job-name", default="dbt_run")
    parser.add_argument("--environment", default="dev")
    args = parser.parse_args(argv)

    job_id = emit_dbt_run_results(
        args.target_dir,
        pipeline_name=args.pipeline_name,
        base_path=args.base_path,
        default_layer=args.default_layer,
        job_name=args.job_name,
        environment=args.environment,
    )
    print(f"[OK] KAGE job_run_id={job_id}")


if __name__ == "__main__":
    main()
