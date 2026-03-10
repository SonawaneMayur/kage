"""
KAGE Databricks Analytics Module
==================================
100% native Databricks implementation for KAGE pipeline monitoring & analytics.

This module provides 10 production-ready SQL queries and utilities for:
- Pipeline health monitoring (SLA tracking, success rates)
- Medallion architecture analytics (layer survival rates, data quality)
- Cost attribution & resource optimization
- Failure root cause analysis
- Performance bottleneck detection
- Data freshness & schema drift monitoring
- Executive dashboard generation
- Delta Live Tables (DLT) pipeline deployment

All queries use Databricks SQL functions and are optimized for JSON log ingestion.

Author: KAGE Analytics Team
Version: 1.0
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from typing import Dict, List, Optional
import json


def load_kage_logs(log_path: str, spark: SparkSession) -> Dict[str, DataFrame]:
    """Load KAGE logs → Databricks optimized"""
    dfs = {}
    for event_type in ["job_run", "task_run", "dataset_event"]:
        path = f"{log_path}/event_type={event_type}"
        dfs[event_type] = (spark.read
                           .format("json")
                           .option("multiline", "true")
                           .load(path))
    return dfs


# 1. **Layer Volume Trends**
def layer_volume_trends(log_path: str) -> str:
    """SQL for Databricks → Records landing→gold"""
    return f"""
    SELECT layer, dt, SUM(record_count) as total_records,
           LAG(SUM(record_count)) OVER (PARTITION BY pipeline_name ORDER BY dt, layer_order) as prev_layer_records,
           ROUND(SUM(record_count)*100.0 / NULLIF(LAG(SUM(record_count)) OVER (PARTITION BY pipeline_name ORDER BY dt, layer_order), 0), 2) as survival_rate_pct
    FROM json.`{log_path}/dataset_event` 
    WHERE event_action = 'WRITE' 
      AND layer IN ('landing', 'bronze', 'silver', 'gold')
    GROUP BY layer, dt, pipeline_name,
      CASE layer WHEN 'landing' THEN 1 WHEN 'bronze' THEN 2 WHEN 'silver' THEN 3 ELSE 4 END as layer_order
    ORDER BY pipeline_name, dt, layer_order
    """


# 2. **Data Lineage Graph**
def data_lineage_graph(log_path: str) -> str:
    return f"""
    SELECT dataset_name, 
           EXPLODE(CAST(upstream_datasets AS ARRAY<STRING>)) as upstream_dataset,
           layer, 
           COUNT(*) as dependency_strength
    FROM json.`{log_path}/dataset_event`
    WHERE upstream_datasets IS NOT NULL
    GROUP BY dataset_name, upstream_dataset, layer
    ORDER BY dependency_strength DESC
    """


# 3. **Pipeline SLA Dashboard**
def pipeline_sla_dashboard(log_path: str) -> str:
    return f"""
    SELECT pipeline_name,
           COUNT(*) as total_runs,
           SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_runs,
           ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as success_pct,
           PERCENTILE_APPROX(duration_ms, 0.9) as p90_duration_ms,
           SUM(total_records_written) as total_volume_gb,
           AVG(duration_ms)/1000/60 as avg_duration_min
    FROM json.`{log_path}/job_run`
    GROUP BY pipeline_name
    ORDER BY success_pct DESC, total_volume_gb DESC
    """


# 4. **Failure Root Cause Analysis**
def failure_root_cause(log_path: str) -> str:
    return f"""
    WITH all_errors AS (
      SELECT pipeline_name, error_message, error_stacktrace, 'job' as error_source FROM json.`{log_path}/job_run` WHERE status = 'FAILED'
      UNION ALL
      SELECT pipeline_name, error_message, error_stacktrace, 'task' FROM json.`{log_path}/task_run` WHERE status = 'FAILED'
      UNION ALL
      SELECT pipeline_name, error_message, error_stacktrace, 'dataset' FROM json.`{log_path}/dataset_event` WHERE error_message IS NOT NULL
    )
    SELECT error_message,
           COUNT(*) as failure_count,
           COLLECT_LIST(DISTINCT pipeline_name) as affected_pipelines,
           COLLECT_LIST(DISTINCT error_source) as error_sources,
           MAX(timestamp) as last_occurred
    FROM all_errors
    GROUP BY error_message
    ORDER BY failure_count DESC
    LIMIT 20
    """


# 5. **Cost Attribution**
def cost_attribution(log_path: str) -> str:
    return f"""
    WITH task_costs AS (
      SELECT t.pipeline_name, t.layer, de.record_count, t.duration_ms,
             CASE t.layer 
               WHEN 'landing' THEN 1 
               WHEN 'bronze' THEN 2 
               WHEN 'silver' THEN 5 
               ELSE 10 END as cost_multiplier
      FROM json.`{log_path}/task_run` t
      JOIN json.`{log_path}/dataset_event` de ON t.task_id = de.task_id 
      WHERE de.event_action = 'WRITE'
    )
    SELECT pipeline_name, layer,
           SUM(record_count * cost_multiplier * duration_ms / 1e9) as estimated_compute_cost,
           SUM(record_count) as total_records,
           AVG(duration_ms/1000) as avg_duration_sec
    FROM task_costs
    GROUP BY pipeline_name, layer
    ORDER BY estimated_compute_cost DESC
    """


# 6. **Data Freshness SLA**
def data_freshness_sla(log_path: str) -> str:
    return f"""
    WITH layer_times AS (
      SELECT job_id, layer, timestamp,
             ROW_NUMBER() OVER (PARTITION BY job_id, layer ORDER BY timestamp) as rn
      FROM json.`{log_path}/dataset_event` 
      WHERE event_action = 'WRITE'
    )
    SELECT pipeline_name,
           MAX(CASE WHEN layer = 'gold' THEN timestamp END) - 
           MAX(CASE WHEN layer = 'landing' THEN timestamp END) as freshness_latency_ms,
           AVG(CASE WHEN layer = 'gold' THEN timestamp END - 
                CASE WHEN layer = 'landing' THEN timestamp END) as avg_freshness_hrs
    FROM layer_times
    WHERE layer IN ('landing', 'gold') AND rn = 1
    GROUP BY pipeline_name
    """


# 7. **Bottleneck Detection**
def bottleneck_detection(log_path: str) -> str:
    return f"""
    SELECT t.task_name, t.pipeline_name, t.layer,
           de.record_count,
           t.duration_ms/1000 as duration_sec,
           ROUND(de.record_count * 1.0 / (t.duration_ms/1000), 0) as records_per_sec,
           NTILE(4) OVER (ORDER BY de.record_count * 1.0 / (t.duration_ms/1000)) as perf_quartile
    FROM json.`{log_path}/task_run` t
    JOIN json.`{log_path}/dataset_event` de ON t.task_id = de.task_id
    WHERE de.event_action = 'WRITE' AND t.duration_ms > 0
    ORDER BY records_per_sec ASC
    LIMIT 20
    """


# 8. **Medallion Health Score**
def medallion_health_score(log_path: str) -> str:
    return f"""
    SELECT pipeline_name,
           SUM(CASE WHEN layer = 'landing' THEN record_count ELSE 0 END) as landing_records,
           SUM(CASE WHEN layer = 'bronze' THEN record_count ELSE 0 END) as bronze_records,
           SUM(CASE WHEN layer = 'silver' THEN record_count ELSE 0 END) as silver_records,
           SUM(CASE WHEN layer = 'gold' THEN record_count ELSE 0 END) as gold_records,
           ROUND(SUM(CASE WHEN layer = 'gold' THEN record_count ELSE 0 END)*100.0 / 
                 NULLIF(SUM(CASE WHEN layer = 'landing' THEN record_count ELSE 0 END), 0), 2) as gold_survival_pct
    FROM json.`{log_path}/dataset_event`
    WHERE event_action = 'WRITE'
      AND layer IN ('landing', 'bronze', 'silver', 'gold')
    GROUP BY pipeline_name
    ORDER BY gold_survival_pct DESC
    """


# 9. **Schema Drift Alerts**
def schema_drift_alerts(log_path: str, days: int = 7) -> str:
    return f"""
    SELECT dataset_name, 
           COUNT(DISTINCT schema_hash) as schema_versions,
           COUNT(*) as total_writes,
           MIN(timestamp) as first_seen,
           MAX(timestamp) as last_seen
    FROM json.`{log_path}/dataset_event`
    WHERE schema_hash IS NOT NULL
      AND timestamp >= date_sub(current_date(), {days})
    GROUP BY dataset_name
    HAVING COUNT(DISTINCT schema_hash) > 1
    ORDER BY schema_versions DESC, total_writes DESC
    """


# 10. **Business Metric Tracking**
def business_metric_tracking(log_path: str, revenue_tables: List[str]) -> str:
    table_list = "', '".join(revenue_tables)
    return f"""
    SELECT dataset_name,
           MAX(timestamp) as last_updated,
           SUM(record_count) as last_7d_volume,
           COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) as error_count_7d,
           ROUND(SUM(record_count)*1.0/NULLIF(MAX(timestamp) - MIN(timestamp), 0), 0) as records_per_hour
    FROM json.`{log_path}/dataset_event`
    WHERE dataset_name IN ('{table_list}')
      AND timestamp >= date_sub(current_date(), 7)
    GROUP BY dataset_name
    ORDER BY last_updated DESC
    """


# Add to existing databricks_analytics.py

def pipeline_ontology(log_path: str) -> str:
    """Generate semantic ONTOLOGY for pipelines, datasets, layers (KAGE UNIQUE)"""
    return f"""
    -- PIPELINE ONTOLOGY: Entities + Relationships
    WITH pipeline_entities AS (
      -- 1. PIPELINES (has many JOBS)
      SELECT DISTINCT pipeline_name as pipeline_id,
             'PIPELINE' as entity_type,
             pipeline_name as entity_name,
             'Business process orchestrating data flows' as description
      FROM json.`{log_path}/job_run`

      UNION ALL

      -- 2. JOBS (belongs to PIPELINE, has many TASKS)
      SELECT DISTINCT concat(pipeline_name, '_', job_name) as entity_id,
             'JOB' as entity_type,
             job_name as entity_name,
             concat('Job in ', pipeline_name, ' pipeline') as description
      FROM json.`{log_path}/job_run`

      UNION ALL 

      -- 3. TASKS (belongs to JOB, processes DATASETS)
      SELECT DISTINCT concat(pipeline_name, '_', job_id, '_', task_name) as entity_id,
             'TASK' as entity_type,
             task_name as entity_name,
             concat('Task in ', layer, ' layer') as description
      FROM json.`{log_path}/task_run`

      UNION ALL

      -- 4. DATASETS (processed by TASKS, has LINEAGE)
      SELECT DISTINCT concat(layer, '_', dataset_name) as entity_id,
             'DATASET' as entity_type,
             dataset_name as entity_name,
             concat(layer, ' layer dataset') as description
      FROM json.`{log_path}/dataset_event` 
      WHERE event_action = 'WRITE'
    ),

    relationships AS (
      -- PIPELINE → owns → JOBS
      SELECT pe1.entity_id as subject, 'OWNS' as relationship, pe2.entity_id as object, '1:N' as cardinality
      FROM pipeline_entities pe1 JOIN json.`{log_path}/job_run` jr ON pe1.entity_name = jr.pipeline_name
      JOIN pipeline_entities pe2 ON pe2.entity_name = jr.job_name AND pe2.entity_type = 'JOB'

      UNION ALL

      -- JOB → executes → TASKS  
      SELECT pe1.entity_id as subject, 'EXECUTES' as relationship, pe2.entity_id as object, '1:N' as cardinality
      FROM pipeline_entities pe1 JOIN json.`{log_path}/task_run` tr ON pe1.entity_name LIKE concat('%', tr.job_id)
      JOIN pipeline_entities pe2 ON pe2.entity_name = tr.task_name AND pe2.entity_type = 'TASK'

      UNION ALL

      -- TASK → processes → DATASETS
      SELECT pe1.entity_id as subject, 'PROCESSES' as relationship, pe2.entity_id as object, '1:N' as cardinality
      FROM pipeline_entities pe1 JOIN json.`{log_path}/task_run` tr ON pe1.entity_name LIKE concat('%', tr.task_id)
      JOIN json.`{log_path}/dataset_event` de ON tr.task_id = de.task_id AND de.event_action = 'WRITE'
      JOIN pipeline_entities pe2 ON pe2.entity_id = concat(de.layer, '_', de.dataset_name)

      UNION ALL

      -- DATASET → sources_from → UPSTREAM DATASETS (LINEAGE)
      SELECT concat(de.layer, '_', de.dataset_name) as subject, 
             'SOURCES_FROM' as relationship, 
             explode(upstream_datasets) as object, 
             'N:1' as cardinality
      FROM json.`{log_path}/dataset_event` de
      WHERE upstream_datasets IS NOT NULL AND event_action = 'WRITE'
    )

    -- ONTOLOGY SUMMARY
    SELECT 'ONTOLOGY' as ontology_type, 
           COUNT(DISTINCT entity_id) as total_entities,
           COLLECT_LIST(DISTINCT entity_type) as entity_types,
           COUNT(DISTINCT subject) as total_relationships
    FROM pipeline_entities

    UNION ALL

    -- ENTITIES CATALOG
    SELECT 'ENTITIES' as ontology_type,
           entity_id, entity_type, entity_name, description
    FROM pipeline_entities

    UNION ALL

    -- RELATIONSHIPS CATALOG  
    SELECT 'RELATIONSHIPS' as ontology_type,
           subject, relationship, object, cardinality
    FROM relationships
    ORDER BY ontology_type, entity_id
    """


def unity_catalog_ontology(log_path: str, catalog: str, schema: str) -> str:
    """Generate Unity Catalog CREATE statements from KAGE logs"""
    return f"""
    -- UNITY CATALOG ONTOLOGY from KAGE logs
    -- Run this in Databricks SQL Editor

    -- 1. PIPELINES Schema
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}_pipelines;

    -- 2. DATASET Entities (Gold layer only)
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}_ontology.datasets (
        dataset_id STRING,
        layer STRING, 
        pipeline_name STRING,
        description STRING,
        upstream_datasets ARRAY<STRING>,
        last_updated TIMESTAMP
    );

    -- 3. PIPELINE Relationships  
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}_ontology.lineage (
        downstream_dataset STRING,
        upstream_dataset STRING,
        pipeline_name STRING,
        layer STRING,
        dependency_strength INT,
        first_seen TIMESTAMP,
        last_seen TIMESTAMP
    );

    -- 4. Populated from KAGE logs (run once)
    INSERT INTO {catalog}.{schema}_ontology.datasets
    SELECT DISTINCT 
        concat(layer, '_', dataset_name) as dataset_id,
        layer,
        pipeline_name,
        concat(layer, ' layer dataset') as description,
        upstream_datasets,
        max(timestamp) as last_updated
    FROM json.`{log_path}/dataset_event`
    WHERE event_action = 'WRITE' AND layer = 'gold';

    INSERT INTO {catalog}.{schema}_ontology.lineage
    SELECT 
        concat(de.layer, '_', de.dataset_name) as downstream_dataset,
        explode(de.upstream_datasets) as upstream_dataset,
        de.pipeline_name,
        de.layer,
        count(*) as dependency_strength,
        min(de.timestamp) as first_seen,
        max(de.timestamp) as last_seen
    FROM json.`{log_path}/dataset_event` de
    WHERE de.upstream_datasets IS NOT NULL AND de.event_action = 'WRITE'
    GROUP BY de.pipeline_name, de.layer, de.dataset_name, explode(de.upstream_datasets);
    """


def semantic_layer_views(log_path: str, catalog: str, schema: str) -> str:
    """Business-friendly semantic views on ontology"""
    return f"""
    -- SEMANTIC LAYER: Business queries on technical ontology
    CREATE OR REPLACE VIEW {catalog}.{schema}.pipeline_health AS
    SELECT p.pipeline_name,
           COUNT(DISTINCT j.job_id) as total_jobs,
           AVG(j.duration_ms)/1000/60 as avg_job_duration_min,
           COUNT(CASE WHEN j.status = 'SUCCESS' THEN 1 END)*100.0/COUNT(*) as success_rate_pct
    FROM (SELECT DISTINCT pipeline_name FROM json.`{log_path}/job_run`) p
    LEFT JOIN json.`{log_path}/job_run` j ON p.pipeline_name = j.pipeline_name
    GROUP BY p.pipeline_name;

    CREATE OR REPLACE VIEW {catalog}.{schema}.data_lineage AS
    SELECT d.dataset_id, d.layer, d.description,
           COLLECT_LIST(l.upstream_dataset) as sources,
           SIZE(COLLECT_LIST(l.upstream_dataset)) as source_count
    FROM {catalog}.{schema}_ontology.datasets d
    LEFT JOIN {catalog}.{schema}_ontology.lineage l ON d.dataset_id = l.downstream_dataset
    GROUP BY d.dataset_id, d.layer, d.description;

    CREATE OR REPLACE VIEW {catalog}.{schema}.medallion_flow AS
    SELECT pipeline_name,
           SUM(CASE layer WHEN 'landing' THEN records ELSE 0 END) landing,
           SUM(CASE layer WHEN 'bronze' THEN records ELSE 0 END) bronze, 
           SUM(CASE layer WHEN 'silver' THEN records ELSE 0 END) silver,
           SUM(CASE layer WHEN 'gold' THEN records ELSE 0 END) gold,
           gold*100.0/nullif(landing,0) as survival_pct
    FROM (
        SELECT pipeline_name, layer, SUM(record_count) records
        FROM json.`{log_path}/dataset_event`
        WHERE event_action = 'WRITE'
        GROUP BY pipeline_name, layer
    ) GROUP BY pipeline_name;
    """


# 🔥 UPDATE kage_dashboard() with ONTOLOGY
def kage_dashboard(log_path: str, spark: SparkSession, revenue_tables: Optional[List[str]] = None,
                   catalog: Optional[str] = None, schema: Optional[str] = None) -> None:
    """Enhanced dashboard WITH ONTOLOGY"""

    print("🎯 KAGE EXECUTIVE DASHBOARD + ONTOLOGY")
    print("=" * 80)

    # Existing 5 dashboards...
    print("\n📊 1. PIPELINE SLAs")
    spark.sql(pipeline_sla_dashboard(log_path)).show(10)

    print("\n🏗️  2. MEDALLION HEALTH")
    spark.sql(medallion_health_score(log_path)).show()

    print("\n🚨 3. TOP FAILURES")
    spark.sql(failure_root_cause(log_path)).show(10)

    print("\n🐌 4. BOTTLENECKS")
    spark.sql(bottleneck_detection(log_path)).show(10)

    print("\n💰 5. COST ATTRIBUTION")
    spark.sql(cost_attribution(log_path)).show(10)

    # 🔥 NEW: ONTOLOGY SECTION
    print("\n🔮 6. PIPELINE ONTOLOGY (KAGE UNIQUE)")
    print("Entities + Relationships extracted from logs:")
    ontology_df = spark.sql(pipeline_ontology(log_path))
    ontology_df.filter(col("ontology_type") == "ONTOLOGY").show(truncate=False)

    print("\n📈 7. DATASET LINEAGE GRAPH")
    ontology_df.filter(col("ontology_type") == "ENTITIES").filter(col("entity_type") == "DATASET").show(20,
                                                                                                        truncate=False)

    if catalog and schema:
        print(f"\n✅ UNITY CATALOG SETUP: {catalog}.{schema}_ontology")
        print("Run this SQL to persist ontology:")
        print(unity_catalog_ontology(log_path, catalog, schema))

    if revenue_tables:
        print("\n💎 8. REVENUE TABLES")
        spark.sql(business_metric_tracking(log_path, revenue_tables)).show()


# DLT Pipeline Ready
def create_dlt_analytics_pipeline(log_path: str, catalog: str, schema: str) -> str:
    """Deploy as DLT pipeline → Scheduled analytics"""
    return f"""
@dlt.table(
    comment = "KAGE Pipeline Health Dashboard"
)
def pipeline_sla():
    return spark.sql("{pipeline_sla_dashboard(log_path)}")

@dlt.table(
    comment = "KAGE Medallion Health Score (Unique)"
)
def medallion_health():
    return spark.sql("{medallion_health_score(log_path)}")

@dlt.table(
    comment = "KAGE Cost Attribution (Unique)"
)
def cost_attribution():
    return spark.sql("{cost_attribution(log_path)}")
    """
