# Databricks notebook source
# MAGIC %pip uninstall -y kage
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/user@gmail.com/packages/kage-1.1.0-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

from kage import KageLogger

# COMMAND ----------

kage_log_path = "/Volumes/demo_group_catalog/logs/logs_vol/pyspark"
unity_catalog = "demo_group_catalog"
schema_name = "logs"

# COMMAND ----------

from kage.utils import databricks_analytics as dbxa

# COMMAND ----------

print("Failure Root Cause Analysis")
display(spark.sql(dbxa.failure_root_cause(kage_log_path)))

print("Failure Root Cause Analysis with AI Resolution Suggestions")
display(spark.sql(dbxa.failure_root_cause(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))

print("Layer Volume Trends")
display(spark.sql(dbxa.layer_volume_trends(kage_log_path)))

print("Layer Volume Trends with AI Resolution Suggestions")
display(spark.sql(dbxa.layer_volume_trends(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))

print("Data Lineage Graph")
display(spark.sql(dbxa.data_lineage_graph(kage_log_path)))

print("Data Lineage Graph with AI Resolution Suggestions")
display(spark.sql(dbxa.data_lineage_graph(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))

print("Pipeline SLA Dashboard")
display(spark.sql(dbxa.pipeline_sla_dashboard(kage_log_path)))

print("Pipeline SLA Dashboard with AI Resolution Suggestions")
display(spark.sql(dbxa.pipeline_sla_dashboard(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))

print("Cost Attribution")
display(spark.sql(dbxa.cost_attribution(kage_log_path)))

print("Cost Attribution with AI Resolution Suggestions")
display(spark.sql(dbxa.cost_attribution(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))

print("Data Freshness SLA")
display(spark.sql(dbxa.data_freshness_sla(kage_log_path)))

print("Data Freshness SLA with AI Resolution Suggestions")
display(spark.sql(dbxa.data_freshness_sla(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))

print("Bottleneck Detection")
display(spark.sql(dbxa.bottleneck_detection(kage_log_path)))

print("Bottleneck Detection with AI Resolution Suggestions")
display(spark.sql(dbxa.bottleneck_detection(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))

print("Medallion Health Score")
display(spark.sql(dbxa.medallion_health_score(kage_log_path)))

print("Medallion Health Score with AI Resolution Suggestions")
display(spark.sql(dbxa.medallion_health_score(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))

print("Schema Drift Alerts")
display(spark.sql(dbxa.schema_drift_alerts(kage_log_path)))

print("Schema Drift Alerts with AI Resolution Suggestions")
display(spark.sql(dbxa.schema_drift_alerts(kage_log_path, model_name='databricks-meta-llama-3-3-70b-instruct')))
