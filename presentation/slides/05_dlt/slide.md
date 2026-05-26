# Spark Declarative Pipelines (DLT / Lakeflow)

```python
import dlt
from kage.integrations.spark_declarative import (
    kage_dlt_table, kage_dlt_expectations,
)

@kage_dlt_table(layer="bronze", upstream_datasets=["landing.orders"])
def bronze_orders():
    return spark.readStream.format("cloudFiles").load("/landing/orders")

@kage_dlt_table(layer="silver", upstream_datasets=["bronze_orders"])
@kage_dlt_expectations(
    ("expect_or_drop", "valid_amount", "amount > 0"),
    ("expect_or_fail", "non_null_id", "order_id IS NOT NULL"),
)
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

- Stacks on top of `@dlt.table` — no DLT rewrites
- Handles positive · negative · streaming · empty · huge-batch cases
