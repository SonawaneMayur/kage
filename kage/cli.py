"""
KAGE command-line entry point.

Subcommands:
    kage ui                 launch the Streamlit observability UI
    kage dbt <target/>      ingest dbt artifacts (alias for python -m kage.integrations.dbt)
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _cmd_ui(args: argparse.Namespace) -> int:
    if args.base_path:
        os.environ["KAGE_LOG_PATH"] = str(Path(args.base_path).resolve())

    try:
        import streamlit  # noqa: F401
    except ImportError:
        print("Streamlit not installed. Install the UI extras:\n"
              "    pip install 'kage[ui]'\n"
              "or:\n"
              "    pip install streamlit polars plotly", file=sys.stderr)
        return 1

    app = Path(__file__).resolve().parent / "ui" / "app.py"
    if not app.exists():
        print(f"UI entry not found at {app}", file=sys.stderr)
        return 1

    cmd = [
        sys.executable, "-m", "streamlit", "run", str(app),
        "--server.port", str(args.port),
        "--server.headless", "true" if args.headless else "false",
        "--browser.gatherUsageStats", "false",
    ]
    print(f"Launching KAGE UI on http://localhost:{args.port}")
    print(f"  Reading events from: {os.environ.get('KAGE_LOG_PATH')}")
    return subprocess.call(cmd)


def _cmd_dbt(args: argparse.Namespace) -> int:
    from .integrations.dbt import emit_dbt_run_results
    job_id = emit_dbt_run_results(
        args.target_dir,
        pipeline_name=args.pipeline_name,
        base_path=args.base_path,
        default_layer=args.default_layer,
        job_name=args.job_name,
        environment=args.environment,
    )
    print(f"[OK] KAGE job_run_id={job_id}")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="kage",
                                     description="KAGE observability CLI.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    ui = sub.add_parser("ui", help="Launch the Streamlit UI.")
    ui.add_argument("--base-path", default=os.environ.get("KAGE_LOG_PATH"),
                    help="Path to the KAGE log directory "
                         "(env KAGE_LOG_PATH or ./kage-logs).")
    ui.add_argument("--port", type=int, default=8501)
    ui.add_argument("--headless", action="store_true",
                    help="Run without auto-opening the browser.")
    ui.set_defaults(func=_cmd_ui)

    dbt = sub.add_parser("dbt", help="Ingest dbt target/ artifacts.")
    dbt.add_argument("target_dir")
    dbt.add_argument("--pipeline-name", default="dbt_project")
    dbt.add_argument("--base-path", default=None)
    dbt.add_argument("--default-layer", default="silver",
                     choices=["landing", "bronze", "silver", "gold"])
    dbt.add_argument("--job-name", default="dbt_run")
    dbt.add_argument("--environment", default="dev")
    dbt.set_defaults(func=_cmd_dbt)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
