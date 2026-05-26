# Declarative `@` API

```python
from kage import configure, pipeline, task, dataset

configure(base_path="./kage-logs", pipeline_name="orders")

@dataset(layer="bronze", dataset_name="bronze_orders",
         upstream_datasets=["raw_orders_api"])
def clean(df):
    return df.filter(df.amount > 0)      # record_count auto-inferred

@task(layer="bronze", task_name="bronze_clean")
def bronze_stage(df):
    return clean(df)

@pipeline("daily_etl")                   # error capture, SUCCESS/FAILED
def run(df):
    return bronze_stage(df)
```

- Auto-inferred record count · auto error capture · auto re-raise
