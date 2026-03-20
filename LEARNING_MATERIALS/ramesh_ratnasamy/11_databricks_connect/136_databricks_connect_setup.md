# Databricks Connect Set-up

## Introduction

Now that we have our local development environment ready, let's actually set up Databricks Connect
and see it in action. In this demo, we'll configure the connection, write some PySpark code locally,
run it against a Databricks cluster, and explore the debugging capabilities that make Databricks
Connect so valuable for data engineers.

The real power here is that you get to use all the IDE features you're used to -- breakpoints,
variable inspection, step-through debugging, auto-complete -- while your Spark code runs on a
full Databricks cluster with access to Unity Catalog, Delta Lake, and all the compute power you
need.

## Setting Up the Connection

### Using DatabricksSession

```python
# The main entry point for Databricks Connect v2
from databricks.connect import DatabricksSession

# Option 1: Use default profile from ~/.databrickscfg
spark = DatabricksSession.builder.getOrCreate()

# Option 2: Specify profile
spark = DatabricksSession.builder \
    .profile("staging") \
    .getOrCreate()

# Option 3: Explicit configuration
spark = DatabricksSession.builder \
    .host("https://myworkspace.cloud.databricks.com") \
    .token("dapi_abc123...") \
    .clusterId("0123-456789-abcdefgh") \
    .getOrCreate()

# Option 4: Use serverless compute
spark = DatabricksSession.builder \
    .host("https://myworkspace.cloud.databricks.com") \
    .token("dapi_abc123...") \
    .serverless(True) \
    .getOrCreate()
```

```
DatabricksSession vs SparkSession:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  In Databricks Notebooks:          In Local IDE:                 │
│                                                                  │
│  from pyspark.sql import           from databricks.connect       │
│      SparkSession                      import DatabricksSession  │
│                                                                  │
│  spark = SparkSession              spark = DatabricksSession     │
│      .builder                          .builder                  │
│      .getOrCreate()                    .getOrCreate()            │
│                                                                  │
│  # After this, the API is IDENTICAL                              │
│  # spark.sql(), spark.read, spark.createDataFrame, etc.          │
│  # all work the same way                                         │
│                                                                  │
│  TIP: Write your code to accept a spark session as a parameter,  │
│  so the same code works in both environments.                    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Demo: Writing and Running PySpark Locally

### Project Structure

```
my_databricks_project/
├── .venv/                          # Virtual environment
├── src/
│   ├── __init__.py
│   ├── session.py                  # Session factory
│   ├── transformations.py          # Business logic
│   └── pipeline.py                 # Main pipeline
├── tests/
│   ├── __init__.py
│   └── test_transformations.py     # Unit tests
├── requirements.txt
└── .env                            # Environment variables (gitignored)
```

### Session Factory

```python
# src/session.py
from databricks.connect import DatabricksSession


def get_spark_session(profile: str = None):
    """Create a DatabricksSession for local development."""
    builder = DatabricksSession.builder

    if profile:
        builder = builder.profile(profile)

    return builder.getOrCreate()
```

### Business Logic

```python
# src/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def enrich_customer_data(customers_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    """Join customers with order aggregations."""

    # Aggregate orders per customer
    order_stats = orders_df.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_spend"),
        F.avg("total_amount").alias("avg_order_value"),
        F.max("order_date").alias("last_order_date")
    )

    # Join with customer data
    enriched = customers_df.join(
        order_stats,
        on="customer_id",
        how="left"
    ).withColumn(
        "customer_tier",
        F.when(F.col("total_spend") > 10000, "premium")
         .when(F.col("total_spend") > 1000, "standard")
         .otherwise("basic")
    )

    return enriched
```

### Main Pipeline

```python
# src/pipeline.py
from src.session import get_spark_session
from src.transformations import enrich_customer_data


def main():
    spark = get_spark_session()

    # Read data from Unity Catalog
    customers = spark.table("production.crm.customers")
    orders = spark.table("production.sales.orders")

    # Apply transformations
    enriched = enrich_customer_data(customers, orders)

    # Show results (during development)
    enriched.show(10)
    print(f"Total rows: {enriched.count()}")

    # Write results back to Delta Lake
    enriched.write \
        .mode("overwrite") \
        .saveAsTable("production.analytics.enriched_customers")

    print("Pipeline completed successfully!")


if __name__ == "__main__":
    main()
```

## Demo: Debugging with Breakpoints

```
IDE Debugging with Databricks Connect:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  1. Set a breakpoint in your code                                │
│     (click in the gutter next to a line number)                  │
│                                                                  │
│     enriched = enrich_customer_data(customers, orders)           │
│  ●  enriched.show(10)       ← breakpoint here                   │
│     print(f"Total rows: {enriched.count()}")                     │
│                                                                  │
│  2. Run in Debug mode (F5 in VS Code)                            │
│                                                                  │
│  3. When execution stops at the breakpoint:                      │
│     - Inspect variables (customers, orders, enriched)            │
│     - Evaluate expressions in the Debug Console                  │
│       >>> enriched.schema                                        │
│       >>> enriched.filter(F.col("customer_tier") == "premium")   │
│                .count()                                          │
│     - Step through code line by line (F10)                       │
│     - Step into functions (F11)                                  │
│     - Continue execution (F5)                                    │
│                                                                  │
│  This is impossible with notebooks alone!                        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Demo: Running Unit Tests

```python
# tests/test_transformations.py
from databricks.connect import DatabricksSession
from src.transformations import enrich_customer_data
import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a shared Spark session for all tests."""
    return DatabricksSession.builder.getOrCreate()


def test_enrich_customer_data(spark):
    """Test customer enrichment logic."""

    # Create test data
    customers_data = [
        (1, "Alice", "us-east"),
        (2, "Bob", "us-west"),
        (3, "Charlie", "eu-west"),
    ]
    customers_df = spark.createDataFrame(
        customers_data, ["customer_id", "name", "region"]
    )

    orders_data = [
        (101, 1, "2025-01-15", 5000.00),
        (102, 1, "2025-02-20", 6000.00),
        (103, 2, "2025-01-10", 500.00),
    ]
    orders_df = spark.createDataFrame(
        orders_data, ["order_id", "customer_id", "order_date", "total_amount"]
    )

    # Run transformation
    result = enrich_customer_data(customers_df, orders_df)

    # Assert results
    assert result.count() == 3

    alice = result.filter(result.customer_id == 1).collect()[0]
    assert alice["total_orders"] == 2
    assert alice["total_spend"] == 11000.00
    assert alice["customer_tier"] == "premium"

    charlie = result.filter(result.customer_id == 3).collect()[0]
    assert charlie["total_orders"] is None
    assert charlie["customer_tier"] == "basic"
```

```bash
# Run tests from the command line
pytest tests/ -v

# Output:
# tests/test_transformations.py::test_enrich_customer_data PASSED
# ========================= 1 passed =========================
```

## Writing Portable Code

```python
# Pattern for code that works BOTH in notebooks and locally

def get_spark():
    """Get Spark session -- works in both environments."""
    try:
        # Try Databricks Connect first (local IDE)
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        # Fall back to regular SparkSession (notebook/cluster)
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


# Use this pattern in your code
spark = get_spark()
df = spark.table("production.analytics.my_table")
```

## Key Exam Points

1. **DatabricksSession** is the entry point for Databricks Connect v2 (replaces SparkSession locally)
2. **Same DataFrame/SQL APIs** work with both DatabricksSession and SparkSession
3. **Connection can be configured** via profile, environment variables, or explicit code
4. **Serverless compute** is supported -- use `.serverless(True)` in the builder
5. **Full IDE debugging** with breakpoints is a major advantage over notebook development
6. **Unit tests run locally** but execute Spark code on the remote cluster
7. **Write portable code** that works in both notebooks and local IDEs
8. **createDataFrame works** locally -- Databricks Connect sends the data to the cluster
9. **Read and write Delta tables** through Unity Catalog just like in a notebook
10. **Best practice**: separate session creation from business logic for testability
