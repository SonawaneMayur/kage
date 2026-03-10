"""
KAGE Databricks Context Detection - Zero-config platform detection
KAGE-PROPRIETARY-2026-v1.1
"""
def detect_databricks_context() -> dict:
    """Auto-detect Databricks runtime context (no import errors outside)"""
    context = {"platform": "pyspark"}
    
    try:
        import IPython
        if str(IPython.get_ipython()) == "<DatabricksKernel>":
            from dbruntime.databricks import dbutils
            ctx = dbutils.notebook.getContext()
            context.update({
                "platform": "databricks",
                "cluster_id": ctx.clusterId().getOrElse(None),
                "notebook_path": ctx.notebookPath().getOrElse(None),
                "job_id": ctx.jobId().getOrElse(None)
            })
    except ImportError:
        pass  # Normal outside Databricks
    except Exception:
        pass  # Graceful fallback
    
    return context
