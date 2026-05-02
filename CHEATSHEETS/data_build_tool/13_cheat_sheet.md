# **13 — dbt Cheat Sheet (Print This)**

> Quick-reference card. CLI, project structure, Jinja, materializations, common patterns. Memorize for interviews.

---

## **1. INSTALL & VERIFY**

```bash
python3 -m venv ~/.venvs/dbt && source ~/.venvs/dbt/bin/activate
pip install dbt-core dbt-<adapter>      # adapters: duckdb, snowflake, bigquery, databricks, postgres, redshift
dbt --version                            # verify
```

---

## **2. CLI — TOP 25 COMMANDS**

| Command | What it does |
|---|---|
| `dbt init <project>` | Scaffold new project |
| `dbt debug` | Validate config + connection |
| `dbt deps` | Install packages from `packages.yml` |
| `dbt parse` | Parse-only (fast syntax check) |
| `dbt compile` | Render Jinja → SQL, no execution |
| `dbt run` | Run models |
| `dbt run --select <selector>` | Run subset |
| `dbt run --full-refresh` | Force rebuild incrementals |
| `dbt test` | Run tests only |
| `dbt build` | Run models + tests + snapshots + seeds in DAG order |
| `dbt seed` | Load CSV seeds |
| `dbt snapshot` | Update snapshots |
| `dbt source freshness` | Check source staleness |
| `dbt docs generate` | Build docs site artifacts |
| `dbt docs serve` | Serve docs at `:8080` |
| `dbt list --select <selector>` | List nodes matching selector |
| `dbt show --select <model> --limit 10` | Print rows from a model |
| `dbt clean` | Delete `target/` and `dbt_packages/` |
| `dbt run-operation <macro>` | Run a macro on demand |
| `dbt --target <name> <command>` | Override target |
| `dbt --vars '{"k":"v"}' <command>` | Pass vars |
| `dbt --log-level debug <command>` | Verbose logging |
| `dbt --profiles-dir <path> <command>` | Override profiles location |
| `dbt --threads N <command>` | Override thread count |
| `dbt run --select state:modified+ --defer --state ./prod` | Slim CI |

---

## **3. SELECTION GRAMMAR**

```bash
dbt run --select my_model            # one model
dbt run --select my_model+           # model + downstream
dbt run --select +my_model           # model + upstream
dbt run --select +my_model+          # full subgraph
dbt run --select my_model+1          # one level downstream
dbt run --select staging             # all models in models/staging/
dbt run --select tag:nightly         # by tag
dbt run --select source:shop+        # downstream of any table in shop source
dbt run --select state:modified+     # changed since saved manifest (Slim CI)
dbt run --select result:error+       # last run errored (retry pattern)
dbt run --select fqn:shop.staging.*  # by fully-qualified name pattern
dbt run --exclude tag:slow           # exclude
dbt run --select "stg_orders fct_*"  # multiple selectors (space-separated = OR)
dbt run --select "stg_orders,tag:hourly"  # comma = AND (intersection)
```

---

## **4. PROJECT STRUCTURE**

```
my_project/
├── dbt_project.yml              # project config
├── packages.yml                 # external packages
├── profiles.yml                 # in ~/.dbt/, NOT committed
│
├── models/
│   ├── staging/<source>/        # stg_<source>__<table>.sql, view
│   ├── intermediate/            # int_<entity>_<verb>.sql, ephemeral or view
│   └── marts/<domain>/          # dim_<entity>.sql, fct_<entity>.sql, table or incremental
│       ├── core/
│       ├── finance/
│       └── marketing/
│
├── tests/
│   ├── generic/                 # custom generic tests
│   └── *.sql                    # singular tests
│
├── macros/                      # reusable Jinja+SQL functions
├── snapshots/                   # SCD2 captures
├── seeds/                       # static CSVs
├── analyses/                    # SQL that compiles, never runs
└── target/                      # generated, gitignored
    ├── compiled/
    ├── run/
    ├── manifest.json
    ├── run_results.json
    └── catalog.json
```

---

## **5. `dbt_project.yml` ESSENTIALS**

```yaml
name: 'my_project'
version: '1.0.0'
profile: 'my_project'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]

clean-targets: ["target", "dbt_packages"]

vars:
  start_date: '2024-01-01'

models:
  my_project:
    +materialized: view
    staging:
      +materialized: view
      +tags: ['staging']
    marts:
      +materialized: table
      +schema: marts
      core:
        +tags: ['core']
        +post-hook: "GRANT SELECT ON {{ this }} TO ROLE analyst"

seeds:
  my_project:
    +schema: seeds
    country_codes:
      +column_types:
        country_code: varchar(2)

snapshots:
  +target_schema: snapshots

on-run-start:
  - "INSERT INTO audit.runs VALUES ('{{ invocation_id }}', '{{ run_started_at }}', 'started')"
on-run-end:
  - "INSERT INTO audit.runs VALUES ('{{ invocation_id }}', current_timestamp, 'finished')"
```

---

## **6. `profiles.yml` (lives in `~/.dbt/`)**

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /tmp/dev.duckdb
      threads: 4
    prod:
      type: snowflake
      account: "{{ env_var('SF_ACCOUNT') }}"
      user: "{{ env_var('SF_USER') }}"
      password: "{{ env_var('SF_PASSWORD') }}"
      role: prod_role
      warehouse: prod_wh
      database: prod_db
      schema: analytics
      threads: 16
```

---

## **7. JINJA QUICK REFERENCE**

```jinja
{# comment, removed at compile #}

{{ expression }}                  {# render result as text #}

{% set x = 10 %}                  {# variable #}

{% if target.name == 'prod' %}    {# conditional #}
  full SQL
{% else %}
  sampled SQL
{% endif %}

{% for col in ['a','b','c'] %}    {# loop #}
  {{ col }}{% if not loop.last %},{% endif %}
{% endfor %}

{%- ... -%}                       {# trim whitespace around tag #}

{{ value | upper | trim }}        {# filters chain with | #}

{% macro my_func(arg1, arg2=10) %}{% endmacro %}    {# define macro #}
{{ my_func('hello') }}                              {# call macro #}
```

---

## **8. dbt-SPECIFIC JINJA**

| Function/Var | Returns | Use |
|---|---|---|
| `ref('model_name')` | Relation | Model dependency |
| `source('src','table')` | Relation | Source dependency |
| `config(materialized='...')` | None | Set compile-time config |
| `var('my_var', 'default')` | Value | Read project var |
| `env_var('NAME', 'default')` | String | Read OS env var |
| `is_incremental()` | Bool | Incremental run guard |
| `target.name` / `.schema` / `.database` / `.type` | String | Current target info |
| `this` | Relation | Current model's name |
| `run_started_at` | Timestamp | Run start |
| `invocation_id` | UUID | Per-run identifier |
| `dbt_version` | String | dbt version |
| `adapter.dispatch('name')` | Macro | Warehouse-specific dispatch |
| `adapter.get_columns_in_relation(rel)` | List | Column metadata |
| `run_query(sql)` | Result | Execute SQL during compile (gate with `if execute`) |
| `log(msg, info=True)` | None | Print to stdout |
| `exceptions.raise_compiler_error('msg')` | — | Fail compile |
| `doc('block_name')` | String | Doc block content |

---

## **9. MATERIALIZATIONS**

```sql
{{ config(materialized='view') }}
{{ config(materialized='table') }}
{{ config(materialized='ephemeral') }}
{{ config(
    materialized='incremental',
    unique_key='order_id',                       -- or list ['a','b']
    incremental_strategy='merge',                -- merge | delete+insert | append | insert_overwrite | microbatch
    on_schema_change='append_new_columns',       -- ignore | fail | append_new_columns | sync_all_columns
    cluster_by=['order_date'],                   -- Snowflake/BigQuery
    partition_by={'field':'date','data_type':'date'},  -- BigQuery
) }}
```

---

## **10. INCREMENTAL TEMPLATE**

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns'
) }}

select * from {{ ref('source_model') }}

{% if is_incremental() %}
  where _ingested_at >= (
    select coalesce(dateadd('day', -3, max(_ingested_at)), '1900-01-01')
    from {{ this }}
  )
{% endif %}
```

---

## **11. SNAPSHOT TEMPLATE**

```sql
{% snapshot my_snap %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=false,
    )
}}

select * from {{ source('shop_raw', 'customers') }}

{% endsnapshot %}
```

Generated columns: `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`.

---

## **12. TESTS**

### **12.1 YAML — generic tests**

```yaml
models:
  - name: stg_customers
    columns:
      - name: customer_id
        data_tests: [unique, not_null]
      - name: status
        data_tests:
          - accepted_values:
              values: ['active','inactive']
      - name: customer_id
        data_tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
```

### **12.2 dbt_utils tests**

```yaml
- dbt_utils.unique_combination_of_columns:
    combination_of_columns: [order_id, line_id]
- dbt_utils.expression_is_true:
    expression: "amount >= 0"
- dbt_utils.equal_rowcount:
    compare_model: ref('legacy_orders')
```

### **12.3 dbt_expectations**

```yaml
- dbt_expectations.expect_column_values_to_be_between:
    min_value: 0
    max_value: 1000000
- dbt_expectations.expect_column_values_to_match_regex:
    regex: '^[0-9]{5}$'
```

### **12.4 Custom generic test**

```sql
{% test valid_email(model, column_name) %}
select {{ column_name }} from {{ model }}
where {{ column_name }} not like '%@%.%'
{% endtest %}
```

### **12.5 Singular test**

`tests/no_negative_amounts.sql`:
```sql
select * from {{ ref('fct_orders') }} where amount < 0
```

### **12.6 Test config**

```yaml
data_tests:
  - not_null:
      config:
        severity: warn         # warn instead of error
        where: "created_at >= '2025-01-01'"
        store_failures: true
        error_if: ">100"
        warn_if: ">10"
```

---

## **13. PACKAGES**

`packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
  - package: dbt-labs/codegen
    version: [">=0.12.0"]
  - git: "https://github.com/org/private-package.git"
    revision: main
```

```bash
dbt deps        # install
```

---

## **14. SEEDS**

```yaml
# dbt_project.yml
seeds:
  my_project:
    country_codes:
      +column_types:
        country_code: varchar(2)
        country_name: varchar(100)
```

```bash
dbt seed                  # load all seeds
dbt seed --select country_codes
dbt seed --full-refresh   # truncate + reload
```

---

## **15. HOOKS**

```yaml
# Per model
{{ config(post_hook="GRANT SELECT ON {{ this }} TO ROLE analyst") }}

# Project-wide
models:
  +post-hook:
    - "GRANT SELECT ON {{ this }} TO ROLE analyst"

# Run-level
on-run-start:
  - "INSERT INTO audit.runs VALUES (...)"
on-run-end:
  - "INSERT INTO audit.runs VALUES (...)"
```

---

## **16. DOCS**

```yaml
models:
  - name: fct_orders
    description: "Order fact table — grain: one row per order"
    columns:
      - name: order_id
        description: "Primary key"
      - name: customer_id
        description: '{{ doc("customer_id") }}'      # reuse doc block
```

`models/docs/_docs.md`:
```markdown
{% docs customer_id %}
The customer's stable identifier from the OLTP system...
{% enddocs %}
```

```bash
dbt docs generate
dbt docs serve --port 8080
```

---

## **17. EXPOSURES**

```yaml
exposures:
  - name: my_dashboard
    type: dashboard               # dashboard | notebook | application | ml
    maturity: high
    url: https://looker.example.com/dashboards/42
    description: "..."
    depends_on:
      - ref('fct_orders')
    owner:
      name: Data Team
      email: data@shop.com
```

---

## **18. CI/CD ESSENTIALS**

```bash
# Slim CI
dbt build --select state:modified+ \
          --defer --state ./prod-state \
          --target ci

# Persist artifacts after prod run
aws s3 cp target/manifest.json s3://artifacts/prod/
aws s3 cp target/run_results.json s3://artifacts/prod/

# Retry only failures
dbt run --select result:error+ --state ./last-run
```

---

## **19. DEBUG WORKFLOW**

```bash
dbt parse                                   # syntax check
dbt compile --select <model>                # render Jinja
cat target/compiled/<project>/<path>.sql    # see compiled SELECT
cat target/run/<project>/<path>.sql         # see SELECT + DDL
dbt show --select <model> --limit 10        # quick row check
dbt run --log-level debug 2> debug.log      # verbose
jq '.results[] | select(.status != "success")' target/run_results.json
```

---

## **20. WAREHOUSE-AWARE PATTERNS**

| Need | Snowflake | BigQuery | Databricks/Spark |
|---|---|---|---|
| Cluster | `cluster_by=['col']` | `cluster_by=['col']` | `liquid_cluster_by=['col']` |
| Partition | n/a (auto-clustered) | `partition_by={...}` | `partition_by=['col']` |
| Increm. strategy | `merge` | `merge` or `insert_overwrite` | `merge` (Delta) |
| External table | `dbt-snowflake` external tables | `external_table` macro | `dbt-databricks` `location_root` |

---

## **21. NAMING CONVENTIONS**

| Layer | Pattern |
|---|---|
| Source | declared in YAML — `source('shop','orders')` |
| Staging | `stg_<source>__<table>` — `stg_shop__orders` |
| Intermediate | `int_<entity>_<verb>` — `int_orders_pivoted` |
| Marts dim | `dim_<entity>` — `dim_customers` |
| Marts fact | `fct_<entity>` — `fct_orders` |
| Snapshot | `<entity>_snap` — `customers_snap` |
| Seed | `<descriptive>` — `country_codes` |

---

## **22. THE 5-STEP "GET STARTED" CHECKLIST**

1. `pip install dbt-core dbt-<adapter>`
2. `dbt init` + configure `profiles.yml`
3. Convert one SQL query into `models/foo.sql` with `{{ ref(...) }}`
4. Add `unique` + `not_null` tests in YAML
5. `dbt build`

---

## **23. THE 4 MATERIALIZATIONS**

| Type | Stored? | Run cost | Read cost | When |
|---|---|---|---|---|
| `view` | No (definition) | Cheap | Recomputes | Light transforms, always-fresh |
| `table` | Yes | Expensive | Cheap | Heavy aggregations consumed often |
| `ephemeral` | No (CTE inline) | Free | Inlined | One-shot helpers |
| `incremental` | Yes | Cheap (after first) | Cheap | Big append/merge fact tables |

---

## **24. THE 4 BUILT-IN TESTS**

| Test | What it checks |
|---|---|
| `unique` | No duplicate non-null values |
| `not_null` | No nulls |
| `accepted_values` | Every value in a given list |
| `relationships` | Foreign key — values exist in another model |

---

## **25. THE TAGLINES**

> **"SQL, engineered."**
>
> **"dbt is SQL + software engineering practices."**
>
> **"Compiles to native warehouse SQL — no engine of its own."**
>
> **"`ref()` is what makes the DAG possible."**
>
> **"In 2026, 'I know dbt' is what 'I know SQL' was a decade ago."**
>
> **"Better SQL. Better Data."**

---

## **NEXT STEP**

Now drill the interview questions.

Go to [`14_interview_questions.md`](14_interview_questions.md).
