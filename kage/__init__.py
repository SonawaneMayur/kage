"""
Kage - Universal Pipeline Observability Platform
KAGE-PROPRIETARY-2026-v1.1 | pip install kage

Pluggable to PySpark, Spark SQL, Pure Python, Scala/Java, dbt, Airflow
"""
__version__ = "1.1.0"

from .core import KageLogger, install_spark_listener
from .schemas import JobRunEvent, TaskRunEvent, DatasetEvent
from .decorators import (
    configure,
    set_default_logger,
    get_default_logger,
    pipeline,
    task,
    dataset,
    kage_pipeline,
    kage_task,
    kage_dataset,
)
from .integrations.spark_declarative import (
    kage_dlt_table,
    kage_dlt_view,
    kage_dlt_expectations,
    is_dlt_available,
)
# Business Analytics (Databricks Native)
from .utils.databricks_analytics import (
    # One-line Executive Dashboard
    kage_dashboard,

    # Core Business Metrics
    pipeline_sla_dashboard,
    medallion_health_score,
    failure_root_cause,
    cost_attribution,
    bottleneck_detection,

    # Data Governance
    pipeline_ontology,
    unity_catalog_ontology,
    data_lineage_graph,

    # SLA Monitoring
    data_freshness_sla,
    schema_drift_alerts,
    business_metric_tracking
)

__all__ = [
    "KageLogger", "install_spark_listener",
    "JobRunEvent", "TaskRunEvent", "DatasetEvent",
    # Declarative @ API
    "configure", "set_default_logger", "get_default_logger",
    "pipeline", "task", "dataset",
    "kage_pipeline", "kage_task", "kage_dataset",
    # Spark Declarative Pipelines (DLT / Lakeflow) integration
    "kage_dlt_table", "kage_dlt_view", "kage_dlt_expectations",
    "is_dlt_available",
    # Analytics
    "kage_dashboard",
    "pipeline_sla_dashboard",
    "medallion_health_score",
    "failure_root_cause",
    "cost_attribution",
    "bottleneck_detection",
    "pipeline_ontology",
    "unity_catalog_ontology",
    "data_lineage_graph",
    "data_freshness_sla",
    "schema_drift_alerts",
    "business_metric_tracking"
]
