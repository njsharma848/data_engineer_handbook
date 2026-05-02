# **03 — Sources, `ref()`, and the DAG**

> **Goal:** Master the two functions that make dbt actually a graph-based tool: `ref()` and `source()`. Understand how dbt builds its DAG from them, and why hardcoding table names is the cardinal dbt sin.

---

## **1. THE MENTAL MODEL**

Imagine you have 50 dbt models. They reference each other. How does dbt know what depends on what? It can't — unless **you tell it** via two functions:

- **`ref('model_name')`** — "I depend on another dbt model in this project."
- **`source('source_name', 'table_name')`** — "I depend on a raw table that lives in the warehouse but isn't dbt-managed."

These two functions are the **only** way dbt discovers dependencies. Hardcoded table names are invisible to dbt's parser. So:

```sql
-- ❌ DAG-invisible — dbt can't know this depends on stg_customers
select * from main.stg_customers

-- ✅ DAG node — dbt records a dependency edge
select * from {{ ref('stg_customers') }}
```

That's it. Internalize this and you understand 80% of why dbt projects look the way they do.

---

## **2. WHY `ref()` EXISTS**

Pre-`ref()`, the analytics SQL world looked like this:

- **No dependency tracking.** Pipelines were ordered by hand: "run `customers.sql` first, then `customer_orders.sql`." When someone added a new step, they updated the orchestration manually. Forgot? Stale data.
- **No environment portability.** `customers` in dev is `dev_db.dev.customers`. In prod it's `prod_db.analytics.customers`. Hardcoded paths broke the moment you switched envs.
- **No safe refactor.** Renaming a table = grep across the codebase, hope you got them all. Miss one and a downstream pipeline silently produces wrong numbers.

`ref()` solves all three:

1. **Dependency edges are automatic.** dbt parses every model file, extracts every `ref()` call, and builds a DAG.
2. **Environment portability.** `ref('customers')` resolves at compile time to whatever schema/database the current target points at.
3. **Safe refactor.** Rename `customers.sql` → `customers_v2.sql` and update the file's name; dbt errors loudly on every still-stale `ref('customers')` until you fix them.

---

## **3. HOW `ref()` WORKS UNDER THE HOOD**

dbt has a two-phase execution model: **parse** and **run**.

### **Phase 1: parse**

When you run any dbt command, dbt first parses the entire project:

1. Walks every `.sql` file under `model-paths`, `snapshot-paths`, etc.
2. Walks every `.yml` schema file.
3. **Renders Jinja in a "discovery" mode** — just enough to find all `ref()` and `source()` calls. The actual SQL isn't executed.
4. Builds an in-memory graph: nodes = models/sources/tests/seeds/snapshots, edges = ref/source dependencies.
5. Writes the graph to `target/manifest.json`.

### **Phase 2: run**

For each node in the graph, in topological order:

1. Render Jinja fully — `ref()` returns a `Relation` object whose string form is the fully-qualified table name (`database.schema.table`).
2. Wrap with the materialization's DDL.
3. Execute against the warehouse.

So at compile time:

```sql
select * from {{ ref('stg_customers') }}
```

becomes:

```sql
select * from "shop"."dbt_alice"."stg_customers"     -- in dev
select * from "shop"."analytics"."stg_customers"     -- in prod
```

The exact string depends on `target.database`, `target.schema`, the model's own schema config, and the macro `generate_schema_name`. **Same source code, different physical references — that's the portability win.**

### **The DAG visualization**

For a project with:

```
sources: shop_raw.customers, shop_raw.orders   ← declared in sources.yml
models:  stg_customers (refs source customers)
         stg_orders    (refs source orders)
         dim_customers (refs stg_customers)
         fct_orders    (refs stg_orders, stg_customers)
```

The DAG is:

```
shop_raw.customers ─┐                    ┌── dim_customers
                    ├── stg_customers ───┤
                    │                    └── fct_orders
shop_raw.orders ────┴── stg_orders ──────┘
```

`dbt docs serve` renders this in a browser. dbt also uses it to:
- Order builds (`dbt run` runs leaves first, roots last).
- Parallelize (anything with no edges between them runs concurrently up to `threads`).
- Power graph selectors: `dbt run --select stg_customers+` runs `stg_customers` and everything downstream.

---

## **4. SYNTAX — `ref()`**

### **4.1 Basic**

```sql
select * from {{ ref('stg_customers') }}
```

Resolves to the model whose filename is `stg_customers.sql`, regardless of which folder it's in. **Filenames must be globally unique** in a dbt project.

### **4.2 With explicit project (cross-project refs, dbt 1.6+)**

```sql
select * from {{ ref('jaffle_shop', 'stg_customers') }}
```

Used when one dbt project consumes models from another (dbt Mesh). Rare unless you're at scale.

### **4.3 With version (dbt 1.6+)**

```sql
select * from {{ ref('stg_customers', v=2) }}
```

References version 2 of a versioned model. dbt supports model versioning via YAML — used to evolve a model's contract without breaking consumers.

### **4.4 `{{ this }}` — self-ref shortcut**

```sql
{{ config(materialized='incremental') }}
select * from {{ source('raw', 'orders') }}
{% if is_incremental() %}
where order_date > (select max(order_date) from {{ this }})
{% endif %}
```

`{{ this }}` resolves to the current model's own warehouse name. Used heavily in incremental models for "where is my high-water mark?"

---

## **5. SYNTAX — `source()`**

A source is a raw table that exists in the warehouse but isn't built by dbt. To use it in a `ref()`-style way, you **declare** it in a YAML file.

### **5.1 Declare sources**

`models/staging/_sources.yml`:

```yaml
version: 2

sources:
  - name: shop_raw                    # logical group name
    database: raw_warehouse           # warehouse where raw data lives
    schema: ecommerce                 # schema in that warehouse
    description: "Production OLTP replica from Fivetran"
    freshness:                        # optional: file 04 covers freshness checks
      warn_after: { count: 12, period: hour }
      error_after: { count: 24, period: hour }
    loaded_at_field: _fivetran_synced

    tables:
      - name: customers
        description: "Customer master from Postgres replica"
        columns:
          - name: id
            description: "Primary key"
            data_tests:
              - unique
              - not_null
      - name: orders
        description: "Orders fact"
        identifier: orders_raw         # actual warehouse table name (if different)
```

### **5.2 Use in a model**

```sql
-- models/staging/stg_customers.sql
select
    id           as customer_id,
    full_name,
    email
from {{ source('shop_raw', 'customers') }}
```

This compiles to `select id as customer_id, full_name, email from raw_warehouse.ecommerce.customers`.

### **5.3 What sources buy you**

| Without `source()` | With `source()` |
|---|---|
| Hardcoded `raw_warehouse.ecommerce.customers` everywhere | Single declaration; references via `source('shop_raw','customers')` |
| Schema migration breaks every model | Update one YAML file |
| Raw tables invisible in dbt's docs/DAG | Raw tables show up as nodes |
| No tests on raw data | Sources can have tests like models |
| No freshness alerting | `dbt source freshness` checks staleness |

---

## **6. LINE-BY-LINE: A SOURCE-CONSUMING MODEL**

```sql
-- models/staging/stg_orders.sql

{{ config(materialized='view', schema='staging') }}     -- Line A

with raw as (                                           -- Line B
    select * from {{ source('shop_raw', 'orders') }}    -- Line C
),

renamed as (                                            -- Line D
    select
        order_id::int          as order_id,
        customer_id::int       as customer_id,
        order_date::date       as order_date,
        amount::decimal(12,2)  as amount,
        status                 as order_status,
        _fivetran_synced       as _ingested_at
    from raw
)

select * from renamed                                   -- Line E
```

**Line-by-line:**

- **Line A:** `view` materialization (cheap, always-fresh) and an explicit `staging` schema (the table will land at `<target_schema>_staging.stg_orders`). Compile-time directive.
- **Line B–C:** A `raw` CTE that reads from the declared source. At compile time, `{{ source('shop_raw', 'orders') }}` becomes `raw_warehouse.ecommerce.orders` (or whatever the declared `database`+`schema`+`identifier` resolves to). The CTE pattern lets you `SELECT *` from raw and never touch raw paths elsewhere in the file.
- **Line D:** A `renamed` CTE doing all the type casting and renaming. **Convention:** every staging model should have a `renamed`/`final` CTE that defines the model's contract — every column the model exposes is here.
- **Line E:** Final `select * from renamed`. Everything downstream sees a clean, typed, renamed table.

**Why this convention exists:** every change to the source schema gets handled in exactly one place — the `renamed` CTE. Downstream models never see the raw column names.

---

## **7. THE FULL E-COMMERCE EXAMPLE**

Building on the project from `01_setup_and_first_model.md`. We'll add proper sources.

### **Step 1.** Materialize "raw" data into DuckDB so we have something to source from:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb <<EOF
CREATE SCHEMA IF NOT EXISTS raw;
CREATE OR REPLACE TABLE raw.customers AS
  SELECT * FROM (VALUES
    (1, 'Alice', 'alice@shop.com', '2025-12-01'),
    (2, 'Bob',   'bob@shop.com',   '2025-12-15'),
    (3, 'Carol', 'carol@shop.com', '2026-01-03'),
    (4, 'Dan',   NULL,             '2026-01-10')
  ) AS t(id, full_name, email, signup_date);

CREATE OR REPLACE TABLE raw.orders AS
  SELECT * FROM (VALUES
    (101, 1, '2026-01-05', 49.99,  'shipped'),
    (102, 1, '2026-01-08', 19.50,  'shipped'),
    (103, 2, '2026-01-09', 102.00, 'pending'),
    (104, 3, '2026-01-12', 8.75,   'shipped'),
    (105, 1, '2026-01-15', 250.00, 'shipped')
  ) AS t(id, customer_id, order_date, amount, status);
EOF
```

### **Step 2.** Declare the sources in `models/_sources.yml`:

```yaml
version: 2

sources:
  - name: shop_raw
    database: shop          # DuckDB filename ("shop.duckdb") becomes db name
    schema: raw
    description: "Raw e-commerce tables loaded by ingestion"
    tables:
      - name: customers
      - name: orders
```

### **Step 3.** Add `models/staging/stg_customers.sql`:

```sql
{{ config(materialized='view') }}

select
    id           as customer_id,
    full_name    as name,
    email,
    signup_date::date as signup_date
from {{ source('shop_raw', 'customers') }}
```

### **Step 4.** Add `models/staging/stg_orders.sql`:

```sql
{{ config(materialized='view') }}

select
    id              as order_id,
    customer_id,
    order_date::date as order_date,
    amount::decimal(12,2) as amount,
    status
from {{ source('shop_raw', 'orders') }}
```

### **Step 5.** Add `models/marts/fct_customer_orders.sql`:

```sql
{{ config(materialized='table') }}

select
    c.customer_id,
    c.name,
    count(o.order_id)        as orders_count,
    sum(o.amount)            as lifetime_value,
    min(o.order_date)        as first_order_date,
    max(o.order_date)        as last_order_date
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o using (customer_id)
group by 1, 2
```

### **Step 6.** Run the project:

```bash
cd ~/work/shop_dbt
dbt run
```

**Expected output:**

```
Found 3 models, 0 data tests, 1 source, 475 macros

1 of 3 START sql view  model main.stg_customers ............ [OK]
2 of 3 START sql view  model main.stg_orders ............... [OK]
3 of 3 START sql table model main.fct_customer_orders ...... [OK]

Done. PASS=3 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=3
```

dbt parsed the sources YAML, parsed the three model files, found the dependency edges (`stg_customers` → `shop_raw.customers`, `stg_orders` → `shop_raw.orders`, `fct_customer_orders` → `stg_customers` + `stg_orders`), and ran them in topological order.

### **Step 7.** Confirm the result:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb "SELECT * FROM main.fct_customer_orders ORDER BY lifetime_value DESC"
```

```
┌─────────────┬───────┬──────────────┬────────────────┬──────────────────┬─────────────────┐
│ customer_id │ name  │ orders_count │ lifetime_value │ first_order_date │ last_order_date │
├─────────────┼───────┼──────────────┼────────────────┼──────────────────┼─────────────────┤
│      1      │ Alice │      3       │     319.49     │   2026-01-05     │   2026-01-15    │
│      2      │ Bob   │      1       │     102.00     │   2026-01-09     │   2026-01-09    │
│      3      │ Carol │      1       │       8.75     │   2026-01-12     │   2026-01-12    │
│      4      │ Dan   │      0       │       NULL     │      NULL        │      NULL       │
└─────────────┴───────┴──────────────┴────────────────┴──────────────────┴─────────────────┘
```

---

## **8. SELECTING NODES BY DAG POSITION**

Once you have a DAG, you can select pieces of it on the CLI. This is huge for dev velocity.

| Selector | Meaning |
|---|---|
| `dbt run --select stg_customers` | Just that model |
| `dbt run --select stg_customers+` | That model + everything downstream |
| `dbt run --select +fct_customer_orders` | That model + everything upstream |
| `dbt run --select +fct_customer_orders+` | Full subgraph through that node |
| `dbt run --select staging` | All models in `models/staging/` |
| `dbt run --select tag:nightly` | All models tagged `nightly` |
| `dbt run --select source:shop_raw+` | Every model downstream of any table in the `shop_raw` source |
| `dbt run --select state:modified+ --state ./prev` | Anything changed since the previous build (CI) |

The graph operators (`+`, `1+`, `+1`) give you precise blast-radius control. File 11 covers `state:modified` for CI.

---

## **9. REAL-WORLD USE CASES**

- **Source freshness alerts.** Declare `loaded_at_field` and freshness thresholds. Run `dbt source freshness` in CI/cron — it queries the source and warns/errors if data is stale. Catches "Fivetran sync broken" before BI users notice.
- **Source-level tests.** A dbt project with no models can still test raw data: declare sources, add tests, run `dbt test --select source:*`. Common in companies with multiple downstream consumers of the same raw tables.
- **Cross-team dbt Mesh.** Team A builds a `users` project; Team B's models do `{{ ref('users', 'dim_users') }}`. dbt 1.6+ supports project-level access controls.
- **Schema migration.** Source moved from `raw.ecommerce` to `raw.production_v2`? Update the `database`/`schema` in `_sources.yml`. One file.

---

## **10. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Every raw table goes through `source()` then `stg_*` then anything else.** Never let a non-staging model reference a source directly.
- **Declare all sources** even if you don't intend to test them yet — gets them in the docs/DAG.
- **Use `identifier:`** when warehouse table name differs from logical name (legacy systems with weird names).
- **Add at least `unique` + `not_null` tests on source primary keys.** Cheap insurance.
- **Track source freshness** for anything BI consumes.

### **Anti-patterns**

- **Hardcoding table names** in a model — `select * from raw.ecommerce.orders`. dbt can't see the dependency, the DAG is wrong, lineage breaks.
- **Two staging models for the same source table** with overlapping column sets — pick one, refactor downstream.
- **Sources defined inline in `dbt_project.yml`** — they belong in YAML schema files in the same folder as the staging models that consume them.
- **`source()` from a marts-layer model** — that's a layering violation. Marts ref staging or intermediate, never raw.

---

## **11. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What's the difference between `ref()` and `source()`?**

**Model answer:** `ref()` references another dbt model in the same project (or another dbt project in dbt Mesh). `source()` references a raw table that exists in the warehouse but isn't dbt-managed — it has to be declared in a `sources:` YAML block first. Both create dependency edges in the DAG; the difference is only that `source()` points to data dbt didn't build, and that `source()` calls take a two-part name (`source('source_name','table_name')`) while `ref()` takes one.

### **Q2. [Foundational] Why is hardcoding table names a problem in dbt?**

**Model answer:** dbt builds its DAG by parsing `ref()` and `source()` calls in your SQL. A hardcoded `select * from raw.orders` is invisible to the parser — dbt won't know that model depends on `raw.orders`. Result: wrong build order, broken lineage in docs, broken CI selectors like `state:modified+`, no portability across dev/prod (the path is locked to one environment), and no safety on rename (dbt can't tell you what to update). Always go through `source()` for raw, `ref()` for everything else.

### **Q3. [Intermediate] How does dbt's parse phase work, and why does it matter?**

**Model answer:** dbt has two phases: parse and run. In parse, dbt walks every file in the project, renders Jinja in a discovery-only mode just to extract `ref()`/`source()` calls and configs, builds the DAG, and writes `target/manifest.json`. In run, it executes nodes in topological order. The parse phase is why you can run `dbt list` or `dbt compile` without touching the warehouse — and why parse errors (unresolved `ref`, missing source) show up before any SQL executes. Partial parsing is an optimization that re-parses only changed files between runs.

### **Q4. [Intermediate] What does `{{ this }}` mean and when do you use it?**

**Model answer:** `{{ this }}` is a Jinja shortcut for the current model's own fully-qualified warehouse name — same thing dbt would emit for a `ref()` to itself. It's mostly used in incremental models (`where event_time > (select max(event_time) from {{ this }})`) to read the model's existing high-water mark. You can't use `ref('self')` because dbt would flag a circular dependency; `{{ this }}` is the official escape hatch.

### **Q5. [Advanced] What's `dbt source freshness` and how would you wire it into production?**

**Model answer:** `dbt source freshness` queries the `loaded_at_field` of every source declared with freshness thresholds and reports any that exceed `warn_after` / `error_after`. In production you'd run `dbt source freshness` as a separate step in your scheduler (Airflow, dbt Cloud, GitHub Actions cron) — every 30–60 min for critical pipelines. The output is `target/sources.json`, which you can integrate with PagerDuty / Slack / OpsGenie. Freshness checks live independently of the model run, so a stale source can alert without blocking transformations (and vice versa).

### **Q6. [Advanced] Walk me through how `ref()` resolves at compile time.**

**Model answer:** `ref('stg_customers')` is a Jinja macro defined in dbt-core. At compile time it looks up the model named `stg_customers` in the manifest, finds its target `database` + `schema` + `alias` (which themselves come from the model's config + `dbt_project.yml` + the `generate_schema_name` macro + the current target), and returns a `Relation` object whose string form is the fully-qualified identifier the warehouse expects. So in dev you might get `"shop"."dbt_alice_staging"."stg_customers"` and in prod `"shop"."analytics_staging"."stg_customers"`. The same model file produces both — that's the portability win.

---

## **12. GOTCHAS**

- **Filenames must be globally unique** in a dbt project. `staging/stg_orders.sql` and `marts/stg_orders.sql` collide — `ref('stg_orders')` is ambiguous. Newer dbt versions error on this; older ones picked silently.
- **`source()` errors at compile time, not run time.** A typo in `source('shop_raw', 'ordres')` shows up immediately on `dbt parse` because the source isn't in `_sources.yml`. Treat that as a feature, not a bug.
- **`source()` does not run the source.** Sources are read-only dependencies — dbt never writes to them. If your "source" is actually a raw table you want dbt to manage, it should be a model or a seed.
- **Sources with no `loaded_at_field` can't have freshness checks.** dbt has no way to know the data's age. You must add an ingestion timestamp column upstream.
- **`ref()` can't reference a seed or snapshot using the wrong name.** `ref('my_seed')` works for a seed; `ref('my_snapshot')` works for a snapshot. The function is unified — but the names must match the file's basename.
- **Cross-project `ref()`** requires dbt Mesh setup (`dependencies.yml` + project access controls). Don't use it casually.

---

## **NEXT STEP**

You can wire models together. Now learn how to make sure the data they produce is actually correct.

Go to [`04_tests.md`](04_tests.md).
