# Databricks notebook source
# MAGIC %md
# MAGIC # KAGE x Spark Declarative Pipelines (DLT / Lakeflow) - Quickstart
# MAGIC
# MAGIC End-to-end medallion pipeline using `@kage_dlt_table` to layer KAGE
# MAGIC observability on top of Databricks' declarative pipeline runtime.
# MAGIC
# MAGIC **Setup steps**
# MAGIC 1. Build the wheel: `python -m build` (from repo root)
# MAGIC 2. Upload `dist/kage-1.1.0-py3-none-any.whl` to your Databricks workspace
# MAGIC 3. Create a DLT pipeline pointing at this notebook
# MAGIC 4. Set the pipeline's library to the uploaded wheel
# MAGIC 5. Run the pipeline; KAGE logs land at `kage_log_path` below

# COMMAND ----------
# MAGIC %pip install /Workspace/Users/user@example.com/packages/kage-1.1.0-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

from kage import configure
from kage.integrations.spark_declarative import (
    kage_dlt_table,
    kage_dlt_view,
    kage_dlt_expectations,
)

# COMMAND ----------
# Configure the shared default KageLogger once per pipeline.
KAGE_LOG_PATH = "/Volumes/demo_catalog/logs/kage/pyspark"

configure(
    base_path=KAGE_LOG_PATH,
    pipeline_name="orders_medallion",
    environment="prod",
    platform="databricks",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Landing -> Bronze (streaming ingest)
# MAGIC
# MAGIC `skip_count=True` is the default - `df.count()` on a streaming DataFrame
# MAGIC would block forever, and on a batch DataFrame it would double the work.

# COMMAND ----------
@kage_dlt_table(
    layer="bronze",
    upstream_datasets=["landing.orders_raw"],
    comment="Raw orders ingested from cloud files",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .load("/Volumes/demo_catalog/landing/orders/")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Bronze -> Silver (quality expectations)

# COMMAND ----------
@kage_dlt_table(
    layer="silver",
    upstream_datasets=["bronze_orders"],
    comment="Validated orders",
)
@kage_dlt_expectations(
    ("expect_or_drop", "valid_amount", "amount > 0"),
    ("expect_or_drop", "valid_currency", "currency IN ('USD','EUR','GBP')"),
    ("expect_or_fail", "non_null_id", "order_id IS NOT NULL"),
)
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
           .withColumn("amount_usd", F.col("amount") * F.col("fx_rate"))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver -> Gold (batch aggregation, record_count enabled)

# COMMAND ----------
@kage_dlt_table(
    layer="gold",
    upstream_datasets=["silver_orders"],
    comment="Daily revenue by region",
    skip_count=False,  # gold tables are small - the extra count is cheap
)
def gold_revenue_by_region():
    return (
        dlt.read("silver_orders")
           .groupBy("region", F.to_date("order_ts").alias("dt"))
           .agg(F.sum("amount_usd").alias("revenue_usd"))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Optional view (lineage only, no materialization)

# COMMAND ----------
@kage_dlt_view(
    layer="gold",
    upstream_datasets=["gold_revenue_by_region"],
    comment="Last-7-day rolling revenue",
)
def gold_revenue_last_7d():
    return (
        dlt.read("gold_revenue_by_region")
           .filter(F.col("dt") >= F.date_sub(F.current_date(), 7))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Inspect KAGE logs after the pipeline runs
# MAGIC
# MAGIC Each table materialization wrote a `dataset_event` row to:
# MAGIC `{KAGE_LOG_PATH}/event_type=dataset_event/dt=YYYY-MM-DD/part-*.jsonl`

# COMMAND ----------
display(
    spark.read.json(f"{KAGE_LOG_PATH}/event_type=dataset_event/")
         .select("layer", "dataset_name", "event_action",
                 "record_count", "upstream_datasets", "custom_fields.status")
         .orderBy("event_timestamp")
)
