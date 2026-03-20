# Deployment to Databricks Workspaces - Demo

## Introduction

Now let's put everything together and actually deploy a Databricks Asset Bundle to a workspace.
This is where the rubber meets the road -- we'll take our bundle configuration, validate it,
deploy it to different targets, and see the resources created in the Databricks workspace. We'll
also look at how to iterate during development, run jobs from the CLI, and manage the full
deployment lifecycle.

This demo follows the workflow you'd use in a real project: develop locally, validate your
configuration, deploy to dev, test, promote to staging, and finally deploy to production.

## Step 1: Initialize a Bundle from a Template

```bash
# Initialize a new bundle from a template
databricks bundle init

# You'll be prompted to choose a template:
# 1. default-python      - Python project with jobs and DLT
# 2. default-sql         - SQL project with DBSQL queries
# 3. dbt-sql             - dbt project integration
# 4. mlops-stacks        - Full MLOps project

# Or initialize from a specific template
databricks bundle init default-python

# Or use a custom template from a Git repo
databricks bundle init https://github.com/company/custom-bundle-template
```

```
After initialization:

my_project/
├── databricks.yml           ← Generated configuration
├── resources/
│   └── my_project_job.yml   ← Sample job definition
├── src/
│   ├── notebook.py          ← Sample notebook
│   └── my_project/
│       ├── __init__.py
│       └── main.py          ← Sample Python module
├── tests/
│   └── main_test.py         ← Sample test
├── setup.py                 ← Python package setup
├── requirements-dev.txt     ← Dev dependencies
└── .gitignore
```

## Step 2: Validate the Bundle

```bash
# Validate the bundle configuration
databricks bundle validate

# Output (success):
# Name: customer_analytics
# Target: dev
# Workspace:
#   Host: https://dev.cloud.databricks.com
#   User: user@company.com
#   Path: /Users/user@company.com/.bundle/customer_analytics/dev
#
# Found 1 job(s), 1 pipeline(s)
# Validation OK!

# Validate for a specific target
databricks bundle validate -t prod

# Common validation errors:
# - Invalid YAML syntax
# - Missing required fields
# - Invalid resource references
# - Undefined variables
```

```
Validation Checks:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  databricks bundle validate                                      │
│                                                                  │
│  ✓ YAML syntax is valid                                          │
│  ✓ All required fields present                                   │
│  ✓ Variable references resolve                                   │
│  ✓ File paths exist                                              │
│  ✓ Target configuration is valid                                 │
│  ✓ Resource definitions are well-formed                          │
│  ✓ No circular dependencies in job tasks                         │
│                                                                  │
│  Run this before every deploy!                                   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Step 3: Deploy to Development

```bash
# Deploy to the default target (dev)
databricks bundle deploy

# Output:
# Uploading bundle files to /Users/user@company.com/.bundle/customer_analytics/dev...
# Deploying resources...
#   Created job: [dev user] Customer Analytics Pipeline (job_id: 123456)
#   Created pipeline: [dev user] Customer DLT Pipeline (pipeline_id: abc123)
# Deployment complete!

# Note the [dev user] prefix -- this is added in development mode
# to avoid conflicts with other developers
```

```
What Happens During Deploy:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  databricks bundle deploy                                        │
│                                                                  │
│  1. VALIDATE configuration                                       │
│     └── Same as 'bundle validate'                                │
│                                                                  │
│  2. UPLOAD source files                                          │
│     └── Notebooks, Python files, SQL files → workspace           │
│                                                                  │
│  3. BUILD artifacts (if any)                                     │
│     └── Python wheels, JARs → uploaded to workspace              │
│                                                                  │
│  4. CREATE or UPDATE resources                                   │
│     ├── Jobs (create new or update existing)                     │
│     ├── DLT Pipelines                                            │
│     ├── ML Experiments                                           │
│     └── Other resources                                          │
│                                                                  │
│  5. TRACK state                                                  │
│     └── Bundle state stored in workspace for future deploys      │
│                                                                  │
│  Subsequent deploys UPDATE existing resources (not recreate)     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Step 4: Run a Job

```bash
# Run a job defined in the bundle
databricks bundle run customer_pipeline_job

# Output:
# Run URL: https://dev.cloud.databricks.com/#job/123456/run/789
# Run ID: 789
#
# 2025-03-20 08:00:01 "ingest_data" RUNNING
# 2025-03-20 08:05:32 "ingest_data" SUCCEEDED
# 2025-03-20 08:05:35 "transform_data" RUNNING
# 2025-03-20 08:12:18 "transform_data" SUCCEEDED
# 2025-03-20 08:12:20 "quality_checks" RUNNING
# 2025-03-20 08:13:45 "quality_checks" SUCCEEDED
#
# Run completed successfully!

# Run with parameter overrides
databricks bundle run customer_pipeline_job \
  --params catalog=test_catalog,schema=test_schema

# Run a DLT pipeline
databricks bundle run customer_dlt_pipeline
```

## Step 5: Deploy to Staging

```bash
# Deploy to staging target
databricks bundle deploy -t staging

# Output:
# Uploading bundle files to /Shared/.bundle/customer_analytics/staging...
# Deploying resources...
#   Created job: Customer Analytics Pipeline (job_id: 234567)
#   Created pipeline: Customer DLT Pipeline (pipeline_id: def456)
# Deployment complete!

# Note: No [dev user] prefix in staging -- resources use their real names
# Variables are resolved with staging values (staging_catalog, etc.)
```

## Step 6: Deploy to Production

```bash
# Deploy to production
databricks bundle deploy -t prod

# Output:
# Uploading bundle files to /Shared/.bundle/customer_analytics/prod...
# Deploying resources...
#   Created job: Customer Analytics Pipeline (job_id: 345678)
#   Created pipeline: Customer DLT Pipeline (pipeline_id: ghi789)
#
# Production deployment:
#   - Schedules are ACTIVE
#   - Running as service principal: prod-deployer
#   - All resources deployed with production settings
#
# Deployment complete!
```

```
Deployment Across Targets:

  LOCAL                     DEV                STAGING              PROD
  ─────                     ───                ───────              ────

  databricks.yml    ──▶   [dev user]         Customer              Customer
  resources/*.yml          Customer            Analytics             Analytics
  src/notebooks/           Analytics            Pipeline              Pipeline
                            Pipeline
                                              staging_catalog       prod_catalog
                           dev_catalog         analytics             analytics
                           analytics
                                              Schedule: active      Schedule: active
                           Schedule: paused    Run as: user         Run as: SP

  Same code, different configuration per target
```

## CI/CD Integration

```yaml
# .github/workflows/deploy.yml

name: Deploy Databricks Bundle

on:
  push:
    branches:
      - main        # Deploy to staging on merge to main
  release:
    types: [published]  # Deploy to prod on release

jobs:
  deploy-staging:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Validate bundle
        run: databricks bundle validate -t staging
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_TOKEN }}

      - name: Deploy to staging
        run: databricks bundle deploy -t staging
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_TOKEN }}

  deploy-prod:
    if: github.event_name == 'release'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy to production
        run: databricks bundle deploy -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
```

## Destroying Deployed Resources

```bash
# Remove all resources deployed by the bundle
databricks bundle destroy -t dev

# Output:
# This will permanently delete the following resources:
#   Job: [dev user] Customer Analytics Pipeline (123456)
#   Pipeline: [dev user] Customer DLT Pipeline (abc123)
#   Files in: /Users/user@company.com/.bundle/customer_analytics/dev
#
# Are you sure? [y/N]: y
#
# Deleting resources...
#   Deleted job: 123456
#   Deleted pipeline: abc123
#   Deleted bundle files
# Destroy complete!

# Use --auto-approve to skip confirmation (for CI/CD)
databricks bundle destroy -t dev --auto-approve
```

## Key Exam Points

1. **`bundle init`** creates a new bundle from a template (default-python, default-sql, etc.)
2. **`bundle validate`** checks configuration without deploying -- always run before deploy
3. **`bundle deploy`** uploads files and creates/updates resources in the workspace
4. **`bundle run`** executes a job or pipeline defined in the bundle
5. **`bundle destroy`** removes all deployed resources from the workspace
6. **Development mode** prefixes resource names with `[dev <username>]` and pauses schedules
7. **Production mode** activates schedules and requires `run_as` service principal
8. **Subsequent deploys update** existing resources -- they don't create duplicates
9. **CI/CD integration** uses environment variables for authentication
10. **`-t <target>`** flag specifies which target to deploy to (dev, staging, prod)
