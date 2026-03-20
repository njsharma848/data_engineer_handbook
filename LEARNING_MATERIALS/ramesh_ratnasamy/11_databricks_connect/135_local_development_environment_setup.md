# Local Development Environment Set-up

## Introduction

Before you can use Databricks Connect, you need to set up your local development environment
properly. This involves installing the right Python version, setting up a virtual environment,
installing the Databricks Connect package, configuring authentication, and making sure your IDE
is ready to go. Let's walk through the entire setup step by step.

Getting this right is important -- most issues people run into with Databricks Connect come from
misconfigured environments, version mismatches, or authentication problems. Follow these steps
carefully and you'll have a smooth experience.

## Prerequisites

```
Local Development Prerequisites:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  1. PYTHON VERSION                                               │
│     Must match the Python version on your Databricks cluster     │
│     - DBR 13.x → Python 3.10                                    │
│     - DBR 14.x → Python 3.10                                    │
│     - DBR 15.x → Python 3.11                                    │
│     Check: python --version                                      │
│                                                                  │
│  2. DATABRICKS CLUSTER                                           │
│     - Must be running DBR 13.0 or later                          │
│     - Can be an all-purpose cluster or serverless compute        │
│     - Cluster must be running when you connect                   │
│                                                                  │
│  3. IDE                                                          │
│     - VS Code (recommended, with Python extension)               │
│     - PyCharm (Professional or Community)                        │
│     - IntelliJ IDEA (with Python plugin)                         │
│     - Any editor that supports Python                            │
│                                                                  │
│  4. AUTHENTICATION                                               │
│     - Databricks personal access token (PAT)                     │
│     - OR OAuth (M2M or U2M)                                      │
│     - OR AWS IAM credentials (for AWS Databricks)                │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Step 1: Install Python and Create Virtual Environment

```bash
# Check your Python version
python --version
# Should match your Databricks Runtime version

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
.venv\Scripts\activate

# Verify you're in the virtual environment
which python
# Should point to .venv/bin/python
```

```
Why a Virtual Environment?

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  Without venv:                                                   │
│  ┌──────────────┐                                                │
│  │ System Python │ ← All packages installed globally             │
│  │ + package A   │   Version conflicts between projects!         │
│  │ + package B   │                                               │
│  │ + databricks  │                                               │
│  └──────────────┘                                                │
│                                                                  │
│  With venv:                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ Project 1    │  │ Project 2    │  │ Project 3    │           │
│  │ .venv/       │  │ .venv/       │  │ .venv/       │           │
│  │ + databricks │  │ + flask      │  │ + django     │           │
│  │   connect    │  │ + pandas     │  │ + numpy      │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│  Isolated! No conflicts between projects.                        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Step 2: Install Databricks Connect

```bash
# Install Databricks Connect v2
# The version should match your Databricks Runtime version
pip install databricks-connect==14.3.*

# For DBR 13.x:
# pip install databricks-connect==13.3.*

# For DBR 15.x:
# pip install databricks-connect==15.1.*

# Verify installation
pip show databricks-connect

# Also install the Databricks SDK (for authentication helpers)
pip install databricks-sdk
```

```
Version Matching:

┌────────────────────────┬────────────────────────────┐
│ Databricks Runtime     │ databricks-connect version │
├────────────────────────┼────────────────────────────┤
│ DBR 13.3 LTS           │ 13.3.*                     │
│ DBR 14.3 LTS           │ 14.3.*                     │
│ DBR 15.1               │ 15.1.*                     │
│ DBR 15.4 LTS           │ 15.4.*                     │
└────────────────────────┴────────────────────────────┘

IMPORTANT: The major.minor version of databricks-connect
MUST match your cluster's Databricks Runtime version!
```

## Step 3: Configure Authentication

### Option A: Databricks CLI Profile (Recommended)

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure a profile
databricks configure --token

# You'll be prompted for:
# Databricks Host: https://<workspace-url>.databricks.com
# Token: dapi_xxxxxxxxxxxxxxxxxxxx

# This creates a profile in ~/.databrickscfg
```

```
~/.databrickscfg file:

[DEFAULT]
host = https://myworkspace.cloud.databricks.com
token = dapi_abc123def456...

[staging]
host = https://staging-workspace.cloud.databricks.com
token = dapi_staging_token...

You can have multiple profiles for different workspaces.
```

### Option B: Environment Variables

```bash
# Set environment variables
export DATABRICKS_HOST="https://myworkspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_abc123def456..."
export DATABRICKS_CLUSTER_ID="0123-456789-abcdefgh"

# Or for serverless:
export DATABRICKS_SERVERLESS_COMPUTE_ID="auto"
```

### Option C: In-Code Configuration

```python
# Configure directly in your Python code
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder \
    .host("https://myworkspace.cloud.databricks.com") \
    .token("dapi_abc123def456...") \
    .clusterId("0123-456789-abcdefgh") \
    .getOrCreate()

# NOTE: Don't hard-code tokens in production code!
# Use environment variables or profiles instead.
```

## Step 4: Set Up Your IDE

### VS Code Setup

```
VS Code Configuration:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  1. Install Extensions:                                          │
│     - Python (Microsoft)                                         │
│     - Databricks (Databricks official extension)                 │
│     - Pylance (for auto-complete)                                │
│                                                                  │
│  2. Select Python Interpreter:                                   │
│     - Cmd/Ctrl + Shift + P → "Python: Select Interpreter"       │
│     - Choose your .venv Python                                   │
│                                                                  │
│  3. Configure Databricks Extension:                              │
│     - Click Databricks icon in sidebar                           │
│     - Connect to your workspace                                  │
│     - Select a cluster                                           │
│                                                                  │
│  4. Project Structure:                                           │
│     my_project/                                                  │
│     ├── .venv/                (virtual environment)              │
│     ├── src/                                                     │
│     │   ├── __init__.py                                          │
│     │   └── my_pipeline.py   (your PySpark code)                │
│     ├── tests/                                                   │
│     │   └── test_pipeline.py (unit tests)                       │
│     ├── requirements.txt     (pip dependencies)                 │
│     └── .databrickscfg       (or use ~/.databrickscfg)          │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### PyCharm Setup

```
PyCharm Configuration:

┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  1. Set Project Interpreter:                                     │
│     Settings → Project → Python Interpreter                      │
│     → Add → Existing Environment → .venv/bin/python              │
│                                                                  │
│  2. Install Databricks Plugin (optional):                        │
│     Settings → Plugins → Marketplace → "Databricks"             │
│                                                                  │
│  3. Configure Run/Debug:                                         │
│     Run → Edit Configurations → Add Python                       │
│     Set environment variables for authentication                 │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Step 5: Validate the Setup

```python
# validate_connection.py
from databricks.connect import DatabricksSession

# Create a Spark session connected to Databricks
spark = DatabricksSession.builder.getOrCreate()

# Test the connection
print(f"Spark version: {spark.version}")

# Run a simple query
df = spark.sql("SELECT current_catalog(), current_schema()")
df.show()

# Read a Delta table
df = spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 5")
df.show()

print("Connection successful!")
```

```
Expected Output:

Spark version: 3.5.0
+-------------------+------------------+
|current_catalog()  |current_schema()  |
+-------------------+------------------+
|hive_metastore     |default           |
+-------------------+------------------+

+----------+-----+----------+----+
|trip_dist  |fare |pickup_zip|..  |
+----------+-----+----------+----+
|1.2       |8.50 |10001     |..  |
|3.5       |15.00|10010     |..  |
+----------+-----+----------+----+

Connection successful!
```

## Common Setup Issues

```
Troubleshooting:

┌─────────────────────────────┬────────────────────────────────────┐
│ Problem                     │ Solution                           │
├─────────────────────────────┼────────────────────────────────────┤
│ "Cluster not found"         │ Check cluster ID, ensure cluster   │
│                             │ is running                         │
├─────────────────────────────┼────────────────────────────────────┤
│ "Authentication failed"     │ Verify token is valid, check host  │
│                             │ URL (include https://)             │
├─────────────────────────────┼────────────────────────────────────┤
│ "Version mismatch"          │ Match databricks-connect version   │
│                             │ to your DBR version                │
├─────────────────────────────┼────────────────────────────────────┤
│ "Python version mismatch"   │ Use same Python version as cluster │
│                             │ (check with python --version)      │
├─────────────────────────────┼────────────────────────────────────┤
│ "Cannot resolve host"       │ Check network/VPN connectivity     │
│                             │ to Databricks workspace            │
├─────────────────────────────┼────────────────────────────────────┤
│ "Module not found"          │ Ensure virtual environment is      │
│                             │ activated and package installed    │
└─────────────────────────────┴────────────────────────────────────┘
```

## Key Exam Points

1. **Python version must match** the Databricks Runtime version on your cluster
2. **Virtual environments** isolate project dependencies and prevent conflicts
3. **databricks-connect package version** must match the DBR major.minor version
4. **Authentication options**: CLI profile (~/.databrickscfg), environment variables, or in-code
5. **Personal access tokens (PAT)** are the simplest auth method for development
6. **DatabricksSession** replaces SparkSession when using Databricks Connect
7. **VS Code + Databricks extension** provides the best integrated experience
8. **Cluster must be running** when you execute code via Databricks Connect
9. **Serverless compute** can be used with Databricks Connect v2 (no cluster needed)
10. **Always validate** your setup with a simple test query before starting development
