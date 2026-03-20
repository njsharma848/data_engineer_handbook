# Introduction to Databricks Asset Bundles

## Introduction

Let's talk about Databricks Asset Bundles -- or DABs for short. If you've worked with any modern
software development workflow, you know that infrastructure-as-code and CI/CD are essential. You
don't manually create resources in production -- you define them in code, version them in Git,
and deploy them through automated pipelines. Databricks Asset Bundles bring this same discipline
to your Databricks projects.

Think of a DAB as a complete, deployable package of everything your Databricks project needs:
notebooks, Python files, SQL files, job definitions, DLT pipeline configurations, cluster specs,
and deployment settings -- all defined in YAML and stored in Git. Instead of manually clicking
through the UI to create jobs and pipelines, you define everything in a bundle and deploy it
with a single command.

## What Are Databricks Asset Bundles?

```
Databricks Asset Bundles:

┌──────────────────────────────────────────────────────────────────┐
│                   DATABRICKS ASSET BUNDLES                       │
│                                                                  │
│   A project format for developing, testing, and deploying        │
│   Databricks resources as code                                   │
│                                                                  │
│   Key Properties:                                                │
│   - Infrastructure-as-code for Databricks                        │
│   - YAML-based configuration (databricks.yml)                    │
│   - Version controlled in Git                                    │
│   - Supports multiple deployment targets (dev, staging, prod)    │
│   - Deploys via Databricks CLI                                   │
│   - Manages jobs, pipelines, clusters, and more                  │
│   - Templates for quick project scaffolding                      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Why Asset Bundles?

```
Without Asset Bundles:                 With Asset Bundles:

┌──────────────────────┐               ┌──────────────────────┐
│ Manual UI clicks     │               │ YAML configuration   │
│ - Create job         │               │ - databricks.yml     │
│ - Configure cluster  │               │ - Version in Git     │
│ - Set schedule       │               │ - Code review        │
│ - Upload notebook    │               │ - CI/CD pipeline     │
│ - Repeat for staging │               │                      │
│ - Repeat for prod    │               │ $ databricks bundle  │
│ - Hope nothing       │               │   deploy -t prod     │
│   breaks...          │               │                      │
│                      │               │ Deploys everything   │
│ Problems:            │               │ consistently across  │
│ - No version control │               │ all environments     │
│ - No reproducibility │               │                      │
│ - Config drift       │               │ Benefits:            │
│ - Manual errors      │               │ ✓ Reproducible       │
│ - Hard to rollback   │               │ ✓ Auditable          │
└──────────────────────┘               │ ✓ Consistent         │
                                       │ ✓ Rollback via Git   │
                                       └──────────────────────┘
```

## What Can Bundles Manage?

```
Databricks Resources Managed by Bundles:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  WORKFLOWS / JOBS                                                │
│  ├── Job definitions (tasks, schedule, clusters)                 │
│  ├── Task dependencies and ordering                              │
│  └── Job parameters and notifications                            │
│                                                                  │
│  DLT PIPELINES                                                   │
│  ├── Pipeline configuration                                      │
│  ├── Source notebooks/files                                      │
│  └── Target schema and catalog                                   │
│                                                                  │
│  ML EXPERIMENTS & MODELS                                         │
│  ├── MLflow experiment definitions                               │
│  ├── Model serving endpoints                                     │
│  └── Model registry entries                                      │
│                                                                  │
│  SOURCE CODE                                                     │
│  ├── Notebooks (.py, .sql, .scala)                               │
│  ├── Python wheel packages                                       │
│  ├── SQL files                                                   │
│  └── Library dependencies                                        │
│                                                                  │
│  INFRASTRUCTURE                                                  │
│  ├── Cluster configurations                                      │
│  ├── Instance pools                                              │
│  └── Permissions and access control                              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Bundle Project Structure

```
Typical Bundle Project:

my_databricks_project/
├── databricks.yml              ← Main bundle configuration
├── resources/
│   ├── jobs.yml                ← Job definitions
│   └── pipelines.yml           ← DLT pipeline definitions
├── src/
│   ├── notebooks/
│   │   ├── ingestion.py        ← Notebook for data ingestion
│   │   ├── transform.py        ← Transformation logic
│   │   └── quality_checks.sql  ← SQL quality checks
│   └── libraries/
│       └── my_package/         ← Python package
│           ├── __init__.py
│           └── utils.py
├── tests/
│   └── test_transforms.py      ← Unit tests
├── fixtures/
│   └── test_data.csv           ← Test data
└── .github/
    └── workflows/
        └── deploy.yml          ← CI/CD pipeline
```

## The databricks.yml File

```yaml
# databricks.yml -- the heart of every bundle

bundle:
  name: customer_analytics_pipeline

# Deployment targets (environments)
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dev-workspace.cloud.databricks.com

  staging:
    workspace:
      host: https://staging-workspace.cloud.databricks.com

  prod:
    mode: production
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    run_as:
      service_principal_name: "prod-deployer-sp"

# Include additional YAML files
include:
  - resources/*.yml
```

## Deployment Targets

```
Deployment Targets:

┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  databricks.yml                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ targets:                                                  │  │
│  │                                                           │  │
│  │   ┌─────────┐   ┌──────────┐   ┌──────────────────────┐  │  │
│  │   │   dev   │   │ staging  │   │       prod           │  │  │
│  │   │         │   │          │   │                      │  │  │
│  │   │ mode:   │   │ mode:    │   │ mode: production     │  │  │
│  │   │ develop │   │ default  │   │                      │  │  │
│  │   │         │   │          │   │ run_as:              │  │  │
│  │   │ Prefixes│   │ Full     │   │  service_principal   │  │  │
│  │   │ resource│   │ deploy   │   │                      │  │  │
│  │   │ names   │   │          │   │ Locked down,         │  │  │
│  │   │ with    │   │          │   │ no interactive dev   │  │  │
│  │   │ [dev    │   │          │   │                      │  │  │
│  │   │  user]  │   │          │   │ Uses service         │  │  │
│  │   │         │   │          │   │ principal identity   │  │  │
│  │   └─────────┘   └──────────┘   └──────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  development mode:                                              │
│  - Resource names prefixed with [dev <user>]                    │
│  - Enables interactive development                              │
│  - Jobs use development mode (no schedule)                      │
│                                                                 │
│  production mode:                                               │
│  - Resources deployed as defined                                │
│  - Schedules are active                                         │
│  - Must use run_as for identity                                 │
│  - Cannot run interactive "bundle run"                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Key Exam Points

1. **Databricks Asset Bundles (DABs)** are infrastructure-as-code for Databricks projects
2. **databricks.yml** is the main configuration file that defines the bundle
3. **Targets** define deployment environments (dev, staging, prod) with different settings
4. **Development mode** prefixes resource names with `[dev <username>]` and pauses schedules
5. **Production mode** requires `run_as` with a service principal and activates schedules
6. **Bundles manage**: jobs, DLT pipelines, notebooks, libraries, ML experiments, and clusters
7. **Git-based workflow** -- all configuration is version controlled and code-reviewed
8. **Databricks CLI** is used to validate, deploy, and run bundles
9. **Templates** provide quick scaffolding for common project types
10. **Include directive** allows splitting configuration across multiple YAML files
