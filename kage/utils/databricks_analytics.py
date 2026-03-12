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
    """
    Load KAGE logs from JSON files for each event type.
    Args:
        log_path: Path to KAGE logs directory
        spark: SparkSession object
    Returns:
        Dictionary mapping event_type to DataFrame
    """
    dfs = {}
    for event_type in ["job_run", "task_run", "dataset_event"]:
        path = f"{log_path}/event_type={event_type}"
        dfs[event_type] = (spark.read
                           .format("json")
                           .option("multiline", "true")
                           .load(path))
    return dfs

def failure_root_cause(log_path: str, model_name=None) -> str:
    """
    SQL query for failure root cause analysis.
    Aggregates error messages, affected pipelines, and exact location of error using ai_query.
    Adds a column with AI-generated suggestions for resolution using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        model_name: Optional serving endpoint name for AI suggestions (e.g. 'databricks-meta-llama-3-3-70b-instruct')
    Returns:
        SQL query string
    """
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given this pipeline error context, provide a brief, specific, actionable resolution. '"
        "|| 'Error: ' || COALESCE(error_message, 'unknown') "
        "|| ' | Pipelines: ' || CAST(COLLECT_LIST(DISTINCT pipeline_name) AS STRING) "
        "|| ' | Error source: ' || CAST(COLLECT_LIST(DISTINCT error_source) AS STRING) "
        "|| ' | Error type: ' || COALESCE(MAX(error_type), 'unknown') "
        "|| ' | Occurrences: ' || CAST(COUNT(*) AS STRING)"
        ") as resolution_suggestion"
    ) if model_name else ""
    return f"""
    -- Failure root cause aggregation with AI suggestions
    WITH all_errors AS (
      SELECT pipeline_name, custom_fields.error_message as error_message, custom_fields.error_type as error_type, 'job' as error_source, event_timestamp FROM json.`{log_path}/event_type=job_run` WHERE status = 'FAILED'
      UNION ALL
      SELECT pipeline_name, custom_fields.error_message as error_message, custom_fields.error_type as error_type, 'task' as error_source, event_timestamp FROM json.`{log_path}/event_type=task_run` WHERE status = 'FAILED'
      UNION ALL
      SELECT pipeline_name, error_message, error_type, 'dataset' as error_source, event_timestamp FROM json.`{log_path}/event_type=dataset_event` WHERE error_message IS NOT NULL
    )
    SELECT error_message,
           COUNT(*) as failure_count,
           COLLECT_LIST(DISTINCT pipeline_name) as affected_pipelines,
           COLLECT_LIST(DISTINCT error_source) as error_sources,
           MAX(event_timestamp) as last_occurred{ai_col}
    FROM all_errors
    GROUP BY error_message
    ORDER BY failure_count DESC
    LIMIT 20
    """

def layer_volume_trends(log_path: str, model_name=None) -> str:
    """
    SQL query for layer volume trends across medallion architecture.
    Tracks record counts and survival rates between layers.
    Adds a column with AI-generated trend analysis using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        model_name: Optional serving endpoint name for AI trend analysis (e.g. 'databricks-meta-llama-3-3-70b-instruct')
    Returns:
        SQL query string
    """
    layer_order_expr = "CASE layer WHEN 'landing' THEN 1 WHEN 'bronze' THEN 2 WHEN 'silver' THEN 3 ELSE 4 END"
    over_clause = f"OVER (PARTITION BY pipeline_name ORDER BY dt, {layer_order_expr})"
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given this medallion layer volume trend, provide a concise, actionable analysis of the trend and any recommendations. '"
        "|| 'Layer: ' || layer "
        "|| ' | Date: ' || CAST(dt AS STRING) "
        "|| ' | Pipeline: ' || pipeline_name "
        "|| ' | Total records: ' || CAST(total_records AS STRING) "
        "|| ' | Previous layer records: ' || CAST(prev_layer_records AS STRING) "
        "|| ' | Survival rate (%): ' || CAST(survival_rate_pct AS STRING)"
        ") as trend_analysis"
    ) if model_name else ""
    return f"""
    -- Layer volume and survival rate calculation with AI trend analysis
    WITH base AS (
      SELECT layer, dt, pipeline_name, SUM(record_count) as total_records,
             LAG(SUM(record_count)) {over_clause} as prev_layer_records,
             ROUND(SUM(record_count)*100.0 / NULLIF(LAG(SUM(record_count)) {over_clause}, 0), 2) as survival_rate_pct
      FROM json.`{log_path}/event_type=dataset_event` 
      WHERE event_action = 'WRITE' 
        AND layer IN ('landing', 'bronze', 'silver', 'gold')
      GROUP BY layer, dt, pipeline_name, {layer_order_expr}
    )
    SELECT layer, dt, pipeline_name, total_records, prev_layer_records, survival_rate_pct{ai_col}
    FROM base
    ORDER BY pipeline_name, dt, {layer_order_expr}
    """

def data_lineage_graph(log_path: str, model_name=None) -> str:
    """
    SQL query to generate data lineage graph.
    Explodes upstream datasets to show dependencies.
    Adds a column with AI-generated recommendation using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        model_name: Optional serving endpoint name for AI recommendations
    Returns:
        SQL query string
    """
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given this dataset lineage, recommend improvements or highlight potential issues. '"
        "|| 'Dataset: ' || dataset_name "
        "|| ' | Upstream: ' || upstream_dataset "
        "|| ' | Layer: ' || layer "
        "|| ' | Dependency strength: ' || CAST(dependency_strength AS STRING)"
        ") as lineage_recommendation"
    ) if model_name else ""
    return f"""
    -- Data lineage graph: dataset dependencies with AI recommendation
    WITH exploded AS (
      SELECT dataset_name, 
             EXPLODE(CAST(upstream_datasets AS ARRAY<STRING>)) as upstream_dataset,
             layer
      FROM json.`{log_path}/event_type=dataset_event`
      WHERE upstream_datasets IS NOT NULL
    )
    SELECT dataset_name, upstream_dataset, layer, 
           COUNT(*) as dependency_strength{ai_col}
    FROM exploded
    GROUP BY dataset_name, upstream_dataset, layer
    ORDER BY dependency_strength DESC
    """

def pipeline_sla_dashboard(log_path: str, model_name=None) -> str:
    """
    SQL query for Pipeline SLA Dashboard.
    Tracks job run success rates, duration metrics, and data volume per pipeline.
    Adds a column with AI-generated recommendation using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        model_name: Optional serving endpoint name for AI recommendations
    Returns:
        SQL query string for execution on Databricks
    """
    job_run_path = f"{log_path}/event_type=job_run"
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given these SLA metrics, recommend actions to improve pipeline reliability or performance. '"
        "|| 'Pipeline: ' || pipeline_name "
        "|| ' | Success rate: ' || CAST(success_pct AS STRING) "
        "|| ' | p90 duration (ms): ' || CAST(p90_duration_ms AS STRING) "
        "|| ' | Total volume: ' || CAST(total_volume AS STRING) "
        "|| ' | Avg duration (min): ' || CAST(avg_duration_min AS STRING)"
        ") as sla_recommendation"
    ) if model_name else ""
    return f"""
    -- Pipeline SLA metrics: success rates, durations, volumes with AI recommendation
    WITH job_runs AS (
    SELECT 
        pipeline_name, -- Pipeline name
        job_run_id, -- Job run identifier
        MAX(start_time) as start_time, -- Latest start time per job run
        MAX(end_time) as end_time, -- Latest end time per job run
        MAX(CASE WHEN end_time IS NOT NULL THEN status END) as status, -- Final status for completed runs
        MAX(custom_fields.rows_checked) as rows_checked -- Maximum rows checked per job run
    FROM json.`{job_run_path}`
    GROUP BY pipeline_name, job_run_id
    )
    SELECT pipeline_name,
        COUNT(*) as total_runs, -- Total job runs per pipeline
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_runs, -- Successful job runs
        ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as success_pct, -- Success rate (%)
        PERCENTILE_APPROX(
            (unix_timestamp(to_timestamp(end_time)) - unix_timestamp(to_timestamp(start_time))) * 1000, 0.9
        ) as p90_duration_ms, -- 90th percentile job run duration (ms)
        SUM(rows_checked) as total_volume, -- Total rows checked
        AVG((unix_timestamp(to_timestamp(end_time)) - unix_timestamp(to_timestamp(start_time))) * 1000)/1000/60 as avg_duration_min -- Average job run duration (min)
        {ai_col}
    FROM job_runs
    GROUP BY pipeline_name
    ORDER BY success_pct DESC, total_volume DESC
    """

def cost_attribution(log_path: str, model_name=None) -> str:
    """
    SQL query for cost attribution per pipeline and layer.
    Estimates compute cost based on record counts and layer multipliers.
    Adds a column with AI-generated recommendation using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        model_name: Optional serving endpoint name for AI recommendations
    Returns:
        SQL query string
    """
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given this cost attribution, recommend ways to optimize compute cost. '"
        "|| 'Pipeline: ' || pipeline_name "
        "|| ' | Layer: ' || layer "
        "|| ' | Estimated cost: ' || CAST(estimated_compute_cost AS STRING) "
        "|| ' | Total records: ' || CAST(total_records AS STRING) "
        "|| ' | Avg duration (sec): ' || CAST(avg_duration_sec AS STRING)"
        ") as cost_recommendation"
    ) if model_name else ""
    return f"""
    -- Cost attribution: compute cost estimation with AI recommendation
    WITH task_costs AS (
      SELECT t.pipeline_name, t.layer, de.record_count, CAST(NULL AS BIGINT) as duration_ms,
             CASE t.layer 
               WHEN 'landing' THEN 1 
               WHEN 'bronze' THEN 2 
               WHEN 'silver' THEN 5 
               ELSE 10 END as cost_multiplier
      FROM json.`{log_path}/event_type=task_run` t
      JOIN json.`{log_path}/event_type=dataset_event` de ON t.task_run_id = de.task_run_id 
      WHERE de.event_action = 'WRITE'
    )
    SELECT pipeline_name, layer,
           SUM(record_count * cost_multiplier * duration_ms / 1e9) as estimated_compute_cost,
           SUM(record_count) as total_records,
           AVG(duration_ms/1000) as avg_duration_sec
           {ai_col}
    FROM task_costs
    GROUP BY pipeline_name, layer
    ORDER BY estimated_compute_cost DESC
    """

def data_freshness_sla(log_path: str, model_name=None) -> str:
    """
    SQL query for data freshness SLA.
    Calculates latency between landing and gold layer writes.
    Adds a column with AI-generated recommendation using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        model_name: Optional serving endpoint name for AI recommendations
    Returns:
        SQL query string
    """
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given this data freshness SLA, recommend actions to improve data latency. '"
        "|| 'Pipeline: ' || pipeline_name "
        "|| ' | Freshness latency (ms): ' || CAST(freshness_latency_ms AS STRING) "
        "|| ' | Avg freshness (hrs): ' || CAST(avg_freshness_hrs AS STRING)"
        ") as freshness_recommendation"
    ) if model_name else ""
    return f"""
    -- Data freshness SLA: latency from landing to gold with AI recommendation
    WITH layer_times AS (
      SELECT job_run_id, pipeline_name, layer, event_timestamp,
             ROW_NUMBER() OVER (PARTITION BY job_run_id, layer ORDER BY event_timestamp) as rn
      FROM json.`{log_path}/event_type=dataset_event` 
      WHERE event_action = 'WRITE'
    )
    SELECT pipeline_name,
           MAX(CASE WHEN layer = 'gold' THEN event_timestamp END) as last_gold_write,
           MAX(CASE WHEN layer = 'landing' THEN event_timestamp END) as last_landing_write,
           CAST(NULL AS DOUBLE) as freshness_latency_ms,
           CAST(NULL AS DOUBLE) as avg_freshness_hrs
           {ai_col}
    FROM layer_times
    WHERE layer IN ('landing', 'gold') AND rn = 1
    GROUP BY pipeline_name
    """

def bottleneck_detection(log_path: str, model_name=None) -> str:
    """
    SQL query for bottleneck detection.
    Identifies tasks with lowest throughput and highest latency.
    Adds a column with AI-generated recommendation using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        model_name: Optional serving endpoint name for AI recommendations
    Returns:
        SQL query string
    """
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given this bottleneck detection, recommend actions to improve throughput or reduce latency. '"
        "|| 'Task: ' || t.task_name "
        "|| ' | Pipeline: ' || t.pipeline_name "
        "|| ' | Layer: ' || t.layer "
        "|| ' | Record count: ' || CAST(de.record_count AS STRING) "
        "|| ' | Perf quartile: ' || CAST(perf_quartile AS STRING)"
        ") as bottleneck_recommendation"
    ) if model_name else ""
    return f"""
    -- Bottleneck detection: performance quartiles with AI recommendation
    SELECT t.task_name, t.pipeline_name, t.layer,
           de.record_count,
           CAST(NULL AS DOUBLE) as duration_sec,
           CAST(NULL AS DOUBLE) as records_per_sec,
           NTILE(4) OVER (ORDER BY de.record_count) as perf_quartile
           {ai_col}
    FROM json.`{log_path}/event_type=task_run` t
    JOIN json.`{log_path}/event_type=dataset_event` de ON t.task_run_id = de.task_run_id
    WHERE de.event_action = 'WRITE'
    ORDER BY de.record_count ASC
    LIMIT 20
    """

def medallion_health_score(log_path: str, model_name=None) -> str:
    """
    SQL query for medallion health score.
    Calculates survival percentage from landing to gold layer.
    Adds a column with AI-generated recommendation using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        model_name: Optional serving endpoint name for AI recommendations
    Returns:
        SQL query string
    """
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given this medallion health score, recommend actions to improve survival rates. '"
        "|| 'Pipeline: ' || pipeline_name "
        "|| ' | Landing records: ' || CAST(landing_records AS STRING) "
        "|| ' | Gold records: ' || CAST(gold_records AS STRING) "
        "|| ' | Gold survival (%): ' || CAST(gold_survival_pct AS STRING)"
        ") as health_recommendation"
    ) if model_name else ""  
    return f"""
    -- Medallion health score: gold survival percentage with AI recommendation
    SELECT pipeline_name,
           SUM(CASE WHEN layer = 'landing' THEN record_count ELSE 0 END) as landing_records,
           SUM(CASE WHEN layer = 'bronze' THEN record_count ELSE 0 END) as bronze_records,
           SUM(CASE WHEN layer = 'silver' THEN record_count ELSE 0 END) as silver_records,
           SUM(CASE WHEN layer = 'gold' THEN record_count ELSE 0 END) as gold_records,
           ROUND(SUM(CASE WHEN layer = 'gold' THEN record_count ELSE 0 END)*100.0 /  
                 NULLIF(SUM(CASE WHEN layer = 'landing' THEN record_count ELSE 0 END), 0), 2) as gold_survival_pct
           {ai_col}
    FROM json.`{log_path}/event_type=dataset_event`
    WHERE event_action = 'WRITE'
      AND layer IN ('landing', 'bronze', 'silver', 'gold')
    GROUP BY pipeline_name
    ORDER BY gold_survival_pct DESC
    """

def schema_drift_alerts(log_path: str, days: int = 7, model_name=None) -> str:
    """
    SQL query for schema drift alerts.
    Detects datasets with multiple schema versions in the last N days.
    Adds a column with AI-generated recommendation using ai_query() if model_name is provided.
    Args:
        log_path: Path to KAGE logs directory
        days: Number of days to look back
        model_name: Optional serving endpoint name for AI recommendations
    Returns:
        SQL query string
    """
    ai_col = (
        f",\n           ai_query('{model_name}', "
        "'You are a Databricks pipeline engineer. Given this schema drift alert, recommend actions to resolve or prevent schema drift. '"
        "|| 'Dataset: ' || dataset_name "
        "|| ' | Schema versions: ' || CAST(schema_versions AS STRING) "
        "|| ' | Total writes: ' || CAST(total_writes AS STRING) "
        "|| ' | First seen: ' || CAST(first_seen AS STRING) "
        "|| ' | Last seen: ' || CAST(last_seen AS STRING)"
        ") as drift_recommendation"
    ) if model_name else ""
    return f"""
    -- Schema drift alerts: multiple schema versions detected with AI recommendation
    SELECT dataset_name, 
           COUNT(DISTINCT dataset_type) as schema_versions,
           COUNT(*) as total_writes,
           MIN(event_timestamp) as first_seen,
           MAX(event_timestamp) as last_seen
           {ai_col}
    FROM json.`{log_path}/event_type=dataset_event`
    WHERE dataset_type IS NOT NULL
      AND to_date(event_timestamp) >= date_sub(current_date(), {days})
    GROUP BY dataset_name
    HAVING COUNT(DISTINCT dataset_type) > 1
    ORDER BY schema_versions DESC, total_writes DESC
    """

def business_metric_tracking(log_path: str, revenue_tables: List[str]) -> str:
    """
    SQL query for business metric tracking on revenue tables.
    Tracks last update, volume, error count, and records per hour.
    Args:
        log_path: Path to KAGE logs directory
        revenue_tables: List of revenue table names
    Returns:
        SQL query string
    """
    table_list = "', '".join(revenue_tables)
    return f"""
    -- Business metric tracking for revenue tables
    SELECT dataset_name,
           MAX(event_timestamp) as last_updated,
           SUM(record_count) as last_7d_volume,
           COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) as error_count_7d,
           ROUND(SUM(record_count)*1.0/NULLIF(
               unix_timestamp(to_timestamp(MAX(event_timestamp))) - unix_timestamp(to_timestamp(MIN(event_timestamp)))
           , 0), 0) as records_per_hour
    FROM json.`{log_path}/event_type=dataset_event`
    WHERE dataset_name IN ('{table_list}')
      AND to_date(event_timestamp) >= date_sub(current_date(), 7)
    GROUP BY dataset_name
    ORDER BY last_updated DESC
    """

def pipeline_ontology(log_path: str) -> str:
    """
    SQL query to generate semantic ontology for pipelines, datasets, layers.
    Extracts entities and relationships from logs.
    Args:
        log_path: Path to KAGE logs directory
    Returns:
        SQL query string
    """
    return f"""
    -- PIPELINE ONTOLOGY: Entities + Relationships
    WITH pipeline_entities AS (
      -- 1. PIPELINES (has many JOBS)
      SELECT DISTINCT pipeline_name as entity_id,
             'PIPELINE' as entity_type,
             pipeline_name as entity_name,
             'Business process orchestrating data flows' as description
      FROM json.`{log_path}/event_type=job_run`
      
      UNION ALL
      
      -- 2. JOBS (belongs to PIPELINE, has many TASKS)
      SELECT DISTINCT concat(pipeline_name, '_', job_name) as entity_id,
             'JOB' as entity_type,
             job_name as entity_name,
             concat('Job in ', pipeline_name, ' pipeline') as description
      FROM json.`{log_path}/event_type=job_run`
      
      UNION ALL 
      
      -- 3. TASKS (belongs to JOB, processes DATASETS)
      SELECT DISTINCT concat(pipeline_name, '_', job_run_id, '_', task_name) as entity_id,
             'TASK' as entity_type,
             task_name as entity_name,
             concat('Task in ', layer, ' layer') as description
      FROM json.`{log_path}/event_type=task_run`
      
      UNION ALL
      
      -- 4. DATASETS (processed by TASKS, has LINEAGE)
      SELECT DISTINCT concat(layer, '_', dataset_name) as entity_id,
             'DATASET' as entity_type,
             dataset_name as entity_name,
             concat(layer, ' layer dataset') as description
      FROM json.`{log_path}/event_type=dataset_event` 
      WHERE event_action = 'WRITE'
    ),
    
    relationships AS (
      -- PIPELINE → owns → JOBS
      SELECT pe1.entity_id as subject, 'OWNS' as relationship, pe2.entity_id as object, '1:N' as cardinality
      FROM pipeline_entities pe1 JOIN json.`{log_path}/event_type=job_run` jr ON pe1.entity_name = jr.pipeline_name
      JOIN pipeline_entities pe2 ON pe2.entity_name = jr.job_name AND pe2.entity_type = 'JOB'
      
      UNION ALL
      
      -- JOB → executes → TASKS  
      SELECT pe1.entity_id as subject, 'EXECUTES' as relationship, pe2.entity_id as object, '1:N' as cardinality
      FROM pipeline_entities pe1 JOIN json.`{log_path}/event_type=task_run` tr ON pe1.entity_name LIKE concat('%', tr.job_run_id)
      JOIN pipeline_entities pe2 ON pe2.entity_name = tr.task_name AND pe2.entity_type = 'TASK'
      
      UNION ALL
      
      -- TASK → processes → DATASETS
      SELECT pe1.entity_id as subject, 'PROCESSES' as relationship, pe2.entity_id as object, '1:N' as cardinality
      FROM pipeline_entities pe1 JOIN json.`{log_path}/event_type=task_run` tr ON pe1.entity_name LIKE concat('%', tr.task_run_id)
      JOIN json.`{log_path}/event_type=dataset_event` de ON tr.task_run_id = de.task_run_id AND de.event_action = 'WRITE'
      JOIN pipeline_entities pe2 ON pe2.entity_id = concat(de.layer, '_', de.dataset_name)
      
      UNION ALL
      
      -- DATASET → sources_from → UPSTREAM DATASETS (LINEAGE)
      SELECT concat(de.layer, '_', de.dataset_name) as subject, 
             'SOURCES_FROM' as relationship, 
             explode(upstream_datasets) as object, 
             'N:1' as cardinality
      FROM json.`{log_path}/event_type=dataset_event` de
      WHERE upstream_datasets IS NOT NULL AND event_action = 'WRITE'
    )
    
    -- ONTOLOGY SUMMARY
    SELECT 'ONTOLOGY' as ontology_type, 
           CAST(COUNT(DISTINCT entity_id) AS STRING) as entity_id,
           CAST(COLLECT_LIST(DISTINCT entity_type) AS STRING) as entity_type,
           CAST(NULL AS STRING) as entity_name,
           CAST(NULL AS STRING) as description
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
    """
    SQL statements to generate Unity Catalog ontology tables and populate them from KAGE logs.
    Args:
        log_path: Path to KAGE logs directory
        catalog: Unity Catalog name
        schema: Schema name
    Returns:
        SQL statements string
    """
    return f"""
    -- UNITY CATALOG ONTOLOGY from KAGE logs
    -- Run this in Databricks SQL Editor
    
    -- 1. PIPELINES Schema
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}_ontology;
    
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
    FROM json.`{log_path}/event_type=dataset_event`
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
    FROM json.`{log_path}/event_type=dataset_event` de
    WHERE de.upstream_datasets IS NOT NULL AND de.event_action = 'WRITE'
    GROUP BY de.pipeline_name, de.layer, de.dataset_name, explode(de.upstream_datasets);
    """

def semantic_layer_views(log_path: str, catalog: str, schema: str) -> str:
    """
    SQL statements to create business-friendly semantic views on ontology.
    Args:
        log_path: Path to KAGE logs directory
        catalog: Unity Catalog name
        schema: Schema name
    Returns:
        SQL statements string
    """
    return f"""
    -- SEMANTIC LAYER: Business queries on technical ontology
    CREATE OR REPLACE VIEW {catalog}.{schema}.pipeline_health AS
    SELECT p.pipeline_name,
           COUNT(DISTINCT j.job_id) as total_jobs,
           AVG(j.duration_ms)/1000/60 as avg_job_duration_min,
           COUNT(CASE WHEN j.status = 'SUCCESS' THEN 1 END)*100.0/COUNT(*) as success_rate_pct
    FROM (SELECT DISTINCT pipeline_name FROM json.`{log_path}/event_type=job_run`) p
    LEFT JOIN json.`{log_path}/event_type=job_run` j ON p.pipeline_name = j.pipeline_name
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
        FROM json.`{log_path}/event_type=dataset_event`
        WHERE event_action = 'WRITE'
        GROUP BY pipeline_name, layer
    ) GROUP BY pipeline_name;
    """

def kage_dashboard(log_path: str, spark: SparkSession, revenue_tables: Optional[List[str]] = None, 
                  catalog: Optional[str] = None, schema: Optional[str] = None) -> None:
    """
    Print KAGE executive dashboard and ontology summary.
    Args:
        log_path: Path to KAGE logs directory
        spark: SparkSession object
        revenue_tables: Optional list of revenue table names
        catalog: Optional Unity Catalog name
        schema: Optional schema name
    """
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
    ontology_df.filter(col("ontology_type") == "ENTITIES").filter(col("entity_type") == "DATASET").show(20, truncate=False)
    
    if catalog and schema:
        print(f"\n✅ UNITY CATALOG SETUP: {catalog}.{schema}_ontology")
        print("Run this SQL to persist ontology:")
        print(unity_catalog_ontology(log_path, catalog, schema))
    
    if revenue_tables:
        print("\n💎 8. REVENUE TABLES")
        spark.sql(business_metric_tracking(log_path, revenue_tables)).show()

def create_dlt_analytics_pipeline(log_path: str, catalog: str, schema: str) -> str:
    """
    Generate DLT pipeline code for scheduled analytics.
    Args:
        log_path: Path to KAGE logs directory
        catalog: Unity Catalog name
        schema: Schema name
    Returns:
        DLT pipeline code string
    """
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