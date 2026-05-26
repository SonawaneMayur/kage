# Databricks notebook source
# MAGIC %md
# MAGIC # KAGE x Spark Declarative Pipelines - Case Handling
# MAGIC
# MAGIC Drop-in patterns for the situations you actually hit in production:
# MAGIC
# MAGIC | Section | Case | What KAGE does |
# MAGIC | --- | --- | --- |
# MAGIC | A | Positive: simple table | logs WRITE, record_count via len/int |
# MAGIC | B | Positive: lineage + expectations | logs upstream + status=SUCCESS |
# MAGIC | C | Negative: function raises | logs error_type/message/stack_trace, re-raises |
# MAGIC | D | Negative: missing upstream table | DLT surfaces error, KAGE captures it |
# MAGIC | E | Extreme: empty DataFrame | record_count=0, no crash |
# MAGIC | F | Extreme: streaming (uncountable) | skip_count, isStreaming respected |
# MAGIC | G | Extreme: huge batch (avoid double count) | skip_count=True (default) |
# MAGIC | H | Extreme: custom record_count_fn (cheap metric) | observe()/agg() instead of count() |

# COMMAND ----------
# MAGIC %pip install /Workspace/Users/user@example.com/packages/kage-1.1.0-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from kage import configure
from kage.integrations.spark_declarative import (
    kage_dlt_table,
    kage_dlt_expectations,
)

KAGE_LOG_PATH = "/Volumes/demo_catalog/logs/kage/pyspark"
configure(base_path=KAGE_LOG_PATH, pipeline_name="cases_demo", platform="databricks")

# COMMAND ----------
# MAGIC %md
# MAGIC ## A. Positive - simple bronze table

# COMMAND ----------
@kage_dlt_table(
    layer="bronze",
    upstream_datasets=["landing.orders_raw"],
    comment="Happy path: ingest raw orders",
)
def orders_bronze():
    return spark.read.json("/Volumes/demo_catalog/landing/orders/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## B. Positive - silver with multi-expectation lineage

# COMMAND ----------
@kage_dlt_table(
    layer="silver",
    upstream_datasets=["orders_bronze", "customers_dim"],
    comment="Enriched, validated orders",
    extra_fields={"owner": "data-platform", "sla_minutes": 30},
)
@kage_dlt_expectations(
    ("expect_or_drop", "amount_positive", "amount > 0"),
    ("expect_or_drop", "known_currency", "currency IN ('USD','EUR','GBP')"),
    ("expect_or_fail", "id_present", "order_id IS NOT NULL"),
)
def orders_silver():
    return (
        dlt.read("orders_bronze")
           .join(dlt.read("customers_dim"), "customer_id", "left")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## C. Negative - function raises an exception
# MAGIC
# MAGIC KAGE logs the failure with full stack_trace then re-raises so DLT marks
# MAGIC the table FAILED. Test this by pointing the source at a missing path.

# COMMAND ----------
@kage_dlt_table(
    layer="bronze",
    upstream_datasets=["landing.missing_table"],
    comment="Intentional failure - source table does not exist",
)
def broken_bronze():
    # FileNotFoundError -> propagates to DLT, but KAGE logs it first
    return spark.read.parquet("/Volumes/demo_catalog/landing/does_not_exist/")

# Inspect the failure event after running:
# spark.read.json(f"{KAGE_LOG_PATH}/event_type=dataset_event/").filter(
#     "custom_fields.status = 'FAILED'"
# ).select("dataset_name", "custom_fields.error_type",
#          "custom_fields.error_message").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## D. Negative - upstream contract violation
# MAGIC
# MAGIC `expect_or_fail` aborts the pipeline. KAGE still emits SUCCESS for the
# MAGIC table that ran cleanly (DLT fails downstream). Pair with
# MAGIC `data_freshness_sla` / `failure_root_cause` analytics queries.

# COMMAND ----------
@kage_dlt_table(layer="silver", upstream_datasets=["orders_bronze"])
@kage_dlt_expectations(
    ("expect_or_fail", "must_have_rows", "order_id IS NOT NULL"),
)
def orders_silver_strict():
    return dlt.read("orders_bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ## E. Extreme - empty DataFrame
# MAGIC
# MAGIC No crash, record_count=0, status=SUCCESS. Useful for pipelines that
# MAGIC sometimes receive zero records (off-hours batches, holiday gaps).

# COMMAND ----------
@kage_dlt_table(
    layer="bronze",
    upstream_datasets=["landing.maybe_empty"],
    skip_count=False,  # we *want* the 0 logged
)
def maybe_empty_bronze():
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])
    return spark.createDataFrame([], schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## F. Extreme - streaming DataFrame
# MAGIC
# MAGIC `df.count()` would block on a streaming DataFrame. KAGE's
# MAGIC `_infer_record_count` checks `isStreaming` and skips counting.
# MAGIC With `skip_count=True` (default) it short-circuits even earlier.

# COMMAND ----------
@kage_dlt_table(
    layer="bronze",
    upstream_datasets=["landing.events"],
    comment="Streaming ingest",
)
def events_bronze_stream():
    return (
        spark.readStream.format("cloudFiles")
             .option("cloudFiles.format", "json")
             .load("/Volumes/demo_catalog/landing/events/")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## G. Extreme - very large batch DataFrame
# MAGIC
# MAGIC The default `skip_count=True` avoids a second pass over the data.
# MAGIC DLT's own pipeline metrics (visible in the UI) carry the true row count;
# MAGIC join those to KAGE events via `dataset_name` if you need it.

# COMMAND ----------
@kage_dlt_table(
    layer="silver",
    upstream_datasets=["orders_bronze"],
    # skip_count=True is the default - kept explicit here for documentation
    skip_count=True,
)
def orders_silver_huge():
    # Imagine 50B rows. Don't call .count().
    return dlt.read("orders_bronze").filter("amount > 0")

# COMMAND ----------
# MAGIC %md
# MAGIC ## H. Extreme - cheap custom record_count via DataFrame.observe()
# MAGIC
# MAGIC Use Spark's `observe` (or an existing aggregation column) to recover an
# MAGIC accurate row count without a second action.

# COMMAND ----------
from pyspark.sql import Observation

def observed_count(df):
    obs = Observation("kage_rows")
    df = df.observe(obs, F.count(F.lit(1)).alias("n"))
    df.write.format("noop").mode("overwrite").save()  # force materialization
    return obs.get["n"]

@kage_dlt_table(
    layer="gold",
    upstream_datasets=["orders_silver"],
    record_count_fn=observed_count,
)
def orders_gold_observed():
    return dlt.read("orders_silver").groupBy("region").agg(F.sum("amount_usd"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Diagnostic query - run after the pipeline
# MAGIC
# MAGIC Surfaces SUCCESS/FAILED for every dataset, with error context inline.

# COMMAND ----------
display(
    spark.read.json(f"{KAGE_LOG_PATH}/event_type=dataset_event/")
         .select(
             "layer", "dataset_name", "event_action", "record_count",
             "upstream_datasets",
             "custom_fields.status",
             "custom_fields.error_type",
             "custom_fields.error_message",
         )
         .orderBy(F.col("event_timestamp").desc())
)
