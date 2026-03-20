# Essential CLI Commands for Asset Bundles

## Introduction

Let's do a quick reference of all the essential Databricks CLI commands you need to know for
working with Asset Bundles. These are the commands you'll use daily as a data engineer managing
Databricks projects with bundles. Think of this as your cheat sheet.

## Command Overview

```
Databricks Bundle CLI Commands:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  LIFECYCLE COMMANDS                                              │
│                                                                  │
│  databricks bundle init          Create a new bundle             │
│  databricks bundle validate      Validate configuration          │
│  databricks bundle deploy        Deploy to a target workspace    │
│  databricks bundle run           Run a job or pipeline           │
│  databricks bundle destroy       Remove deployed resources       │
│                                                                  │
│  INFORMATION COMMANDS                                            │
│                                                                  │
│  databricks bundle summary       Show bundle configuration       │
│  databricks bundle schema        Show YAML schema reference      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Command Details

### bundle init

```bash
# Initialize a new bundle project
databricks bundle init

# Initialize from a specific template
databricks bundle init default-python
databricks bundle init default-sql
databricks bundle init dbt-sql
databricks bundle init mlops-stacks

# Initialize from a custom Git template
databricks bundle init https://github.com/company/my-template

# Initialize in a specific directory
databricks bundle init default-python --output-dir ./my-project
```

### bundle validate

```bash
# Validate the default target
databricks bundle validate

# Validate a specific target
databricks bundle validate -t staging
databricks bundle validate -t prod

# Validate with variable overrides
databricks bundle validate -t prod --var="catalog=test_catalog"
```

### bundle deploy

```bash
# Deploy to default target
databricks bundle deploy

# Deploy to a specific target
databricks bundle deploy -t staging
databricks bundle deploy -t prod

# Deploy with variable overrides
databricks bundle deploy -t prod --var="catalog=custom_catalog"

# Force deploy (overwrite existing resources)
databricks bundle deploy --force
```

### bundle run

```bash
# Run a job
databricks bundle run my_job_name

# Run a DLT pipeline
databricks bundle run my_pipeline_name

# Run with parameters
databricks bundle run my_job_name --params key1=value1,key2=value2

# Run against a specific target
databricks bundle run my_job_name -t staging

# Run and don't wait for completion
databricks bundle run my_job_name --no-wait
```

### bundle destroy

```bash
# Destroy resources in default target
databricks bundle destroy

# Destroy a specific target
databricks bundle destroy -t dev

# Skip confirmation prompt
databricks bundle destroy -t dev --auto-approve
```

### bundle summary

```bash
# Show resolved bundle configuration
databricks bundle summary

# Show summary for a specific target
databricks bundle summary -t prod

# Output includes:
# - Resolved variables
# - Resource IDs (after deployment)
# - Workspace paths
# - Target configuration
```

## Common Workflow

```
Daily Development Workflow:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  1. Make code changes                                            │
│     └── Edit notebooks, Python files, YAML config                │
│                                                                  │
│  2. Validate                                                     │
│     └── databricks bundle validate                               │
│                                                                  │
│  3. Deploy to dev                                                │
│     └── databricks bundle deploy                                 │
│                                                                  │
│  4. Test                                                         │
│     └── databricks bundle run my_job                             │
│                                                                  │
│  5. Iterate (repeat 1-4)                                         │
│                                                                  │
│  6. Commit and push to Git                                       │
│     └── git add . && git commit && git push                      │
│                                                                  │
│  7. CI/CD deploys to staging                                     │
│     └── databricks bundle deploy -t staging                      │
│                                                                  │
│  8. CI/CD deploys to production                                  │
│     └── databricks bundle deploy -t prod                         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Quick Reference Table

```
┌──────────────────────────────┬─────────────────────────────────────┐
│ What you want to do          │ Command                             │
├──────────────────────────────┼─────────────────────────────────────┤
│ Start a new project          │ databricks bundle init              │
│ Check config is valid        │ databricks bundle validate          │
│ Deploy to workspace          │ databricks bundle deploy            │
│ Deploy to production         │ databricks bundle deploy -t prod    │
│ Run a job                    │ databricks bundle run <job_name>    │
│ Run a pipeline               │ databricks bundle run <pipe_name>   │
│ Run with params              │ bundle run <name> --params k=v      │
│ View bundle details          │ databricks bundle summary           │
│ Delete deployed resources    │ databricks bundle destroy           │
│ Override a variable          │ bundle deploy --var="key=value"     │
│ View YAML schema             │ databricks bundle schema            │
└──────────────────────────────┴─────────────────────────────────────┘
```

## Key Exam Points

1. **`bundle init`** -- scaffolds a new project from a template
2. **`bundle validate`** -- checks YAML syntax and configuration without deploying
3. **`bundle deploy`** -- uploads files and creates/updates resources
4. **`bundle run`** -- executes a specific job or pipeline by name
5. **`bundle destroy`** -- deletes all deployed resources from the workspace
6. **`-t <target>` flag** -- specifies the deployment target (dev, staging, prod)
7. **`--var` flag** -- overrides variables from the command line
8. **`--no-wait` flag** -- starts a run without waiting for completion
9. **`--auto-approve` flag** -- skips confirmation prompts (useful in CI/CD)
10. **Workflow order**: init → validate → deploy → run → (iterate) → destroy
