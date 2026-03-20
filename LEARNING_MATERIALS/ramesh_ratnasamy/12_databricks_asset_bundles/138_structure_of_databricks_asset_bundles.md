# Structure of Databricks Asset Bundles

## Introduction

Now let's look at the structure of a Databricks Asset Bundle in more detail. Understanding the
anatomy of a bundle is important because the configuration is how you define everything --
your jobs, pipelines, clusters, permissions, and deployment settings. It's all YAML, and it
follows a specific structure that the Databricks CLI understands.

The key file is `databricks.yml`, which is the entry point for every bundle. From there, you
can include additional YAML files to keep things organized. Let's break down each section.

## Bundle Configuration Anatomy

```
databricks.yml Structure:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  bundle:          ← Bundle metadata (name, etc.)                 │
│                                                                  │
│  variables:       ← Reusable variables across the config         │
│                                                                  │
│  workspace:       ← Default workspace settings                   │
│                                                                  │
│  artifacts:       ← Build artifacts (Python wheels, JARs)        │
│                                                                  │
│  include:         ← Additional YAML files to include             │
│                                                                  │
│  resources:       ← Databricks resources to deploy               │
│    jobs:          ← Workflow/job definitions                      │
│    pipelines:     ← DLT pipeline definitions                     │
│    experiments:   ← MLflow experiment definitions                 │
│    models:        ← ML model definitions                         │
│    schemas:       ← UC schema definitions                        │
│                                                                  │
│  targets:         ← Deployment environments                      │
│    dev:           ← Development target                            │
│    staging:       ← Staging target                                │
│    prod:          ← Production target                             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Complete Example: databricks.yml

```yaml
# databricks.yml

# 1. BUNDLE METADATA
bundle:
  name: customer_analytics

# 2. VARIABLES (reusable across the configuration)
variables:
  catalog:
    default: dev_catalog
  schema:
    default: analytics
  warehouse_id:
    default: "abc123def456"

# 3. DEFAULT WORKSPACE SETTINGS
workspace:
  root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}

# 4. INCLUDE additional configuration files
include:
  - resources/*.yml

# 5. TARGETS (deployment environments)
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dev.cloud.databricks.com
    variables:
      catalog: dev_catalog

  staging:
    workspace:
      host: https://staging.cloud.databricks.com
    variables:
      catalog: staging_catalog

  prod:
    mode: production
    workspace:
      host: https://prod.cloud.databricks.com
    run_as:
      service_principal_name: "prod-deployer"
    variables:
      catalog: prod_catalog
      schema: analytics
```

## Resource Definitions

### Job Definition

```yaml
# resources/jobs.yml

resources:
  jobs:
    customer_pipeline_job:
      name: "Customer Analytics Pipeline"
      schedule:
        quartz_cron_expression: "0 0 8 * * ?"  # Daily at 8 AM
        timezone_id: "America/New_York"

      job_clusters:
        - job_cluster_key: "pipeline_cluster"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            num_workers: 4
            node_type_id: "i3.xlarge"
            data_security_mode: "SINGLE_USER"

      tasks:
        - task_key: "ingest_data"
          job_cluster_key: "pipeline_cluster"
          notebook_task:
            notebook_path: ../src/notebooks/ingestion.py

        - task_key: "transform_data"
          depends_on:
            - task_key: "ingest_data"
          job_cluster_key: "pipeline_cluster"
          notebook_task:
            notebook_path: ../src/notebooks/transform.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}

        - task_key: "quality_checks"
          depends_on:
            - task_key: "transform_data"
          job_cluster_key: "pipeline_cluster"
          sql_task:
            file:
              path: ../src/notebooks/quality_checks.sql
            warehouse_id: ${var.warehouse_id}

      email_notifications:
        on_failure:
          - team@company.com
```

### DLT Pipeline Definition

```yaml
# resources/pipelines.yml

resources:
  pipelines:
    customer_dlt_pipeline:
      name: "Customer DLT Pipeline"
      target: "${var.catalog}.${var.schema}"
      catalog: ${var.catalog}

      libraries:
        - notebook:
            path: ../src/notebooks/dlt_definitions.py

      clusters:
        - label: "default"
          autoscale:
            min_workers: 1
            max_workers: 4

      configuration:
        "spark.sql.shuffle.partitions": "auto"

      continuous: false
      development: true  # Overridden per target
```

## Variable Substitution

```yaml
# Variables can be used throughout the configuration with ${var.name}

variables:
  catalog:
    default: dev_catalog
    description: "The Unity Catalog to use"

  notification_email:
    default: dev-team@company.com

# Usage in resources:
resources:
  jobs:
    my_job:
      name: "ETL Job - ${var.catalog}"
      tasks:
        - task_key: "run_etl"
          notebook_task:
            base_parameters:
              catalog: ${var.catalog}
      email_notifications:
        on_failure:
          - ${var.notification_email}

# Override per target:
targets:
  prod:
    variables:
      catalog: prod_catalog
      notification_email: prod-alerts@company.com
```

```
Variable Resolution:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  Built-in Variables:                                             │
│  ${bundle.name}                    → "customer_analytics"        │
│  ${bundle.target}                  → "dev" / "staging" / "prod" │
│  ${workspace.current_user.userName}→ "user@company.com"         │
│  ${workspace.root_path}            → deployment root path        │
│                                                                  │
│  Custom Variables:                                               │
│  ${var.catalog}                    → value from variables block  │
│  ${var.schema}                     → overridden per target       │
│                                                                  │
│  Resolution Order:                                               │
│  1. Target-level variable override                               │
│  2. Top-level variable default                                   │
│  3. CLI flag (--var="key=value")                                 │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Artifacts (Python Wheels)

```yaml
# Build and deploy Python packages

artifacts:
  my_python_package:
    type: whl
    path: ./src/libraries/my_package
    build: python setup.py bdist_wheel

# Reference in a job task:
resources:
  jobs:
    my_job:
      tasks:
        - task_key: "run_package"
          python_wheel_task:
            package_name: my_package
            entry_point: main
          libraries:
            - whl: ../dist/*.whl
```

## Multi-File Organization

```
Splitting Configuration Across Files:

databricks.yml              ← Main config (bundle, targets, variables)
  │
  ├── include:
  │     - resources/*.yml
  │
  ├── resources/
  │     ├── jobs.yml         ← All job definitions
  │     ├── pipelines.yml    ← All DLT pipeline definitions
  │     └── experiments.yml  ← ML experiment definitions
  │
  └── src/                   ← Source code referenced by resources
        ├── notebooks/
        └── libraries/

Benefits:
- Smaller, focused files
- Easier code reviews
- Team members can work on different resources
- Clear separation of concerns
```

## Key Exam Points

1. **databricks.yml** is the entry point and must exist in the project root
2. **Bundle sections**: bundle, variables, workspace, artifacts, include, resources, targets
3. **Resources include**: jobs, pipelines, experiments, models, schemas
4. **Variables** use `${var.name}` syntax and can be overridden per target
5. **Built-in variables**: `${bundle.name}`, `${bundle.target}`, `${workspace.current_user.userName}`
6. **Include directive** allows splitting config across multiple YAML files
7. **Job definitions** support tasks, dependencies, clusters, schedules, and notifications
8. **Artifacts** build and deploy Python wheel packages
9. **Target-level overrides** let you customize variables and settings per environment
10. **Relative paths** in resource definitions are relative to the YAML file location
