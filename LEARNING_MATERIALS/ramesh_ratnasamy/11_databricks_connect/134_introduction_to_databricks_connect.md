# Introduction to Databricks Connect

## Introduction

Alright, let's talk about Databricks Connect. If you've ever wished you could write and debug
Spark code in your favorite local IDE -- like VS Code, PyCharm, or IntelliJ -- and have it
execute on a Databricks cluster, that's exactly what Databricks Connect does. It's a thin client
library that lets you connect your local development environment to a remote Databricks cluster,
so you get the power of distributed Spark compute without leaving your local editor.

Think about the typical data engineering workflow without Databricks Connect: you write code
locally, push it to a Databricks notebook or upload it as a job, run it, check the results, fix
bugs, and repeat. That's slow and painful. With Databricks Connect, you write code in your local
IDE with full auto-complete, debugging, and testing support, and the code runs directly on a
Databricks cluster. It's the best of both worlds.

## What Is Databricks Connect?

```
Databricks Connect Overview:

┌──────────────────────────────────────────────────────────────────┐
│                    DATABRICKS CONNECT                             │
│                                                                  │
│   A thin client library that connects your local IDE             │
│   to a remote Databricks cluster for Spark execution             │
│                                                                  │
│   Key Properties:                                                │
│   - Run Spark code from your local machine                       │
│   - Execute on remote Databricks clusters                        │
│   - Full IDE support (auto-complete, debugging, breakpoints)     │
│   - Supports Python (PySpark) and Scala                          │
│   - Uses Spark Connect protocol (Databricks Connect v2)          │
│   - No need to install full Spark locally                        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## How It Works

```
Databricks Connect Architecture:

  LOCAL MACHINE                           DATABRICKS
  ─────────────                           ──────────

  ┌──────────────────────┐                ┌──────────────────────┐
  │  Your IDE             │                │  Databricks Cluster  │
  │  (VS Code, PyCharm)  │                │                      │
  │                       │                │  ┌────────────────┐  │
  │  ┌─────────────────┐  │   Spark        │  │  Spark Driver  │  │
  │  │  Your PySpark   │  │   Connect      │  │                │  │
  │  │  Code           │──┼───Protocol────▶│  │  Executes your │  │
  │  │                 │  │   (gRPC)       │  │  Spark code    │  │
  │  │  from pyspark   │  │                │  │                │  │
  │  │  .sql import    │  │                │  └────────┬───────┘  │
  │  │  SparkSession   │  │                │           │          │
  │  └─────────────────┘  │                │  ┌────────▼───────┐  │
  │                       │                │  │  Spark Workers │  │
  │  ┌─────────────────┐  │                │  │  (distributed  │  │
  │  │  databricks-    │  │                │  │   execution)   │  │
  │  │  connect        │  │                │  └────────────────┘  │
  │  │  (pip package)  │  │                │                      │
  │  └─────────────────┘  │                │  Unity Catalog       │
  │                       │                │  Delta Lake Tables   │
  └──────────────────────┘                └──────────────────────┘

  Only the PLAN is sent to the cluster.
  Data processing happens entirely on the cluster.
  Results are sent back to your local machine.
```

## Databricks Connect v2 vs v1

```
Version Comparison:

┌─────────────────────────┬──────────────────────┬──────────────────────┐
│ Feature                 │ Connect v1 (Legacy)  │ Connect v2 (Current) │
├─────────────────────────┼──────────────────────┼──────────────────────┤
│ Protocol                │ Custom               │ Spark Connect (gRPC) │
│ Spark version           │ Spark 3.x            │ Spark 3.4+ / DBR 13+│
│ Package                 │ databricks-connect   │ databricks-connect   │
│ Full Spark locally      │ Yes (heavy)          │ No (thin client)     │
│ Package size            │ ~200MB               │ ~20MB                │
│ Unity Catalog support   │ Limited              │ Full                 │
│ Serverless support      │ No                   │ Yes                  │
│ Debugging               │ Limited              │ Full IDE debugging   │
│ API compatibility       │ Most Spark APIs      │ Most Spark APIs      │
│ Recommended             │ No (deprecated)      │ Yes                  │
└─────────────────────────┴──────────────────────┴──────────────────────┘

v2 is a THIN CLIENT -- it sends query plans to the cluster,
not the full Spark runtime. Much lighter and more reliable.
```

## What You Can and Cannot Do

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  CAN DO with Databricks Connect:                                 │
│  ✓ Run PySpark DataFrames, SQL queries, Spark SQL                │
│  ✓ Read/write Delta Lake tables                                  │
│  ✓ Access Unity Catalog (catalogs, schemas, tables)              │
│  ✓ Use Spark structured streaming                                │
│  ✓ Debug with breakpoints in your IDE                            │
│  ✓ Run unit tests locally with pytest                            │
│  ✓ Use Python libraries alongside PySpark                        │
│                                                                  │
│  CANNOT DO with Databricks Connect:                              │
│  ✗ Use RDD APIs (low-level Spark)                                │
│  ✗ Access SparkContext directly                                  │
│  ✗ Use custom JVM/Scala UDFs (Python UDFs work)                  │
│  ✗ Run Databricks-specific utilities (dbutils, display)          │
│  ✗ Access DBFS directly (use Unity Catalog volumes instead)      │
│  ✗ Use MLlib (some limitations)                                  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Use Cases

```
When to Use Databricks Connect:

┌────────────────────────────────────────────────────────────────┐
│                                                                │
│  1. LOCAL DEVELOPMENT                                          │
│     Write and test Spark code in your favorite IDE             │
│     with auto-complete, linting, and type checking             │
│                                                                │
│  2. DEBUGGING                                                  │
│     Set breakpoints, step through code, inspect variables      │
│     -- things you can't easily do in notebooks                 │
│                                                                │
│  3. CI/CD INTEGRATION                                          │
│     Run Spark-based tests in CI/CD pipelines                   │
│     against a Databricks cluster                               │
│                                                                │
│  4. APPLICATION DEVELOPMENT                                    │
│     Build applications that need to interact with              │
│     Databricks data (e.g., web apps, microservices)            │
│                                                                │
│  5. TEAM COLLABORATION                                         │
│     Use standard Git workflows, code reviews, and              │
│     version control with Spark code                            │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

## Key Exam Points

1. **Databricks Connect** is a thin client library that connects local IDEs to remote Databricks clusters
2. **Spark Connect protocol** (gRPC) is used in v2 -- only query plans are sent, not data
3. **v2 is lightweight** (~20MB) compared to v1 (~200MB) -- no full Spark installation needed
4. **Supports PySpark and Scala** for local development with remote execution
5. **Full IDE debugging** -- breakpoints, step-through, variable inspection
6. **Cannot use RDD APIs** or SparkContext directly -- DataFrame and SQL APIs only
7. **Unity Catalog is fully supported** in v2
8. **dbutils is NOT available** through Databricks Connect
9. **Requires DBR 13.0+** for Databricks Connect v2
10. **Best for**: local development, debugging, CI/CD, and application development workflows
