# **02 — Models and Materializations**

> **Goal:** Understand what a dbt model *is*, and master the four materialization strategies (`view`, `table`, `ephemeral`, `incremental`). The choice you make here directly impacts cost, freshness, and pipeline runtime.

---

## **1. WHAT IS A MODEL?**

A **model** is a `.sql` file under `models/` that contains exactly one `SELECT` statement. dbt takes that SELECT, wraps it in a materialization-specific DDL (`CREATE TABLE AS …`, `CREATE VIEW AS …`, etc.), and runs it against your warehouse.

```
models/customers.sql           ← the file you write
        │
        │ dbt compile
        ▼
target/compiled/.../customers.sql      ← Jinja resolved, pure SQL SELECT
        │
        │ dbt run (wraps with DDL)
        ▼
target/run/.../customers.sql           ← CREATE TABLE main.customers AS (SELECT …)
        │
        │ executed against warehouse
        ▼
warehouse: shop.main.customers ✅
```

**Three things to internalize:**

1. **A model is a SELECT.** No DDL, no INSERT, no UPDATE in the file. dbt generates DDL for you. If you find yourself writing `CREATE TABLE` inside a model file, you're misusing dbt.
2. **A model becomes one object** — usually a table or view in the warehouse. Its name in the warehouse defaults to the filename (with materialization-specific naming).
3. **The materialization is a deployment strategy, not a SQL feature.** Same SELECT can be materialized as `view`, `table`, `ephemeral`, or `incremental` — you're picking how dbt deploys it.

---

## **2. MENTAL MODEL OF MATERIALIZATIONS**

A materialization answers: **"Should this SELECT result be stored, computed every time it's read, or merged into an existing table?"**

| Materialization | Stored? | What runs on `dbt run`? | What runs on read? |
|---|---|---|---|
| **`view`** | No (SQL definition stored) | `CREATE OR REPLACE VIEW` (cheap) | The full SELECT every time |
| **`table`** | Yes | `CREATE OR REPLACE TABLE AS …` (expensive) | A simple `SELECT *` (cheap) |
| **`ephemeral`** | No (inlined as a CTE) | Nothing — gets compiled into downstream models | Nothing direct — only via downstream |
| **`incremental`** | Yes | First run: full table build. Later runs: only new rows merged in | A simple `SELECT *` (cheap) |

The trade-off matrix:

| You care about | Pick |
|---|---|
| Always-fresh data, infrequent reads | `view` |
| Fast reads, infrequent writes | `table` |
| Modular SQL with no warehouse object | `ephemeral` |
| Huge fact table, append-only or merge-able | `incremental` |

---

## **3. HOW IT WORKS UNDER THE HOOD**

A materialization in dbt is itself a **macro** — a piece of Jinja+SQL that wraps your SELECT with DDL. dbt ships with the four built-in ones, and you can define your own (rare).

The pseudocode of the `table` materialization is roughly:

```jinja
{% materialization table, default %}
    {% set tmp_relation = ... %}
    {% set target_relation = ... %}

    -- 1. Build a temp table with the new data
    {% call statement('main') %}
        CREATE OR REPLACE TABLE {{ tmp_relation }} AS (
            {{ sql }}            -- your SELECT
        )
    {% endcall %}

    -- 2. Atomically swap temp → target
    {{ adapter.rename_relation(tmp_relation, target_relation) }}
{% endmaterialization %}
```

This explains a lot:

- Why each materialization can have completely different behavior (it's a macro per materialization).
- Why dbt is **idempotent** — every run drops/replaces the target, leaving you in a known state.
- Why dbt can ship adapters (Snowflake, BigQuery, etc.) that override these macros for warehouse-specific syntax.

> **Interview-grade phrasing:** "A materialization is just a macro that wraps the model's SELECT in DDL. That's why dbt is fundamentally a templater — there's no engine, only compilation."

---

## **4. THE FOUR MATERIALIZATIONS — DEEP DIVE**

### **4.1 `view` — the default**

```sql
{{ config(materialized='view') }}

select customer_id, name, email from {{ ref('raw_customers') }}
```

**What dbt issues to the warehouse:**

```sql
CREATE OR REPLACE VIEW main.customers_v AS (
    select customer_id, name, email from main.raw_customers
);
```

**When to use:**
- Lightweight transformations (renames, filters).
- Always-fresh requirement (every read recomputes).
- Cheap to build, expensive to query repeatedly.

**When NOT to use:**
- Heavy joins, aggregations — the cost is paid on every read.
- Anything fed into a BI dashboard hit hundreds of times per day.

### **4.2 `table` — full rebuild**

```sql
{{ config(materialized='table') }}

select
    customer_id,
    count(*) as orders,
    sum(amount) as lifetime_value
from {{ ref('orders') }}
group by 1
```

**What dbt issues:**

```sql
CREATE OR REPLACE TABLE main.customer_lifetime AS (
    select customer_id, count(*) as orders, sum(amount) as lifetime_value
    from main.orders
    group by 1
);
```

**When to use:**
- Heavy aggregations / joins consumed many times.
- Data that doesn't change between dbt runs.

**When NOT to use:**
- Tables larger than your warehouse can rebuild quickly. A 1B-row table rebuilt every hour will burn money — go incremental instead.

### **4.3 `ephemeral` — no warehouse object**

```sql
{{ config(materialized='ephemeral') }}

select
    customer_id,
    lower(trim(email)) as email_clean
from {{ ref('raw_customers') }}
```

**What dbt issues to the warehouse:** **nothing.** The model is never created. Instead, when a downstream model says `{{ ref('customers_clean') }}`, dbt inlines this SELECT as a CTE.

So if a downstream model is:

```sql
select * from {{ ref('customers_clean') }} where email_clean like '%@gmail.com'
```

dbt compiles it to:

```sql
with __dbt__cte__customers_clean as (
    select customer_id, lower(trim(email)) as email_clean
    from main.raw_customers
)
select * from __dbt__cte__customers_clean where email_clean like '%@gmail.com'
```

**When to use:**
- Logical staging step you don't want to physicalize.
- Reusable transformation you want inlined for performance.

**When NOT to use:**
- Anything used by 3+ downstream models — they each inline the same CTE, which kills query plans.
- Anything with heavy aggregation — repeated inlining means repeated work.
- When you'd want to test it (you can test ephemeral models, but errors point to the downstream model and are confusing).

### **4.4 `incremental` — append/merge new rows only**

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select * from {{ ref('raw_orders') }}

{% if is_incremental() %}
  where order_date > (select max(order_date) from {{ this }})
{% endif %}
```

**Two-mode behavior:**

- **First run** (table doesn't exist): the `{% if is_incremental() %}` block is skipped. dbt issues `CREATE TABLE AS SELECT * FROM raw_orders`.
- **Every subsequent run**: the WHERE filter activates. dbt builds a temp table of just the new rows, then merges them into the target using `unique_key='order_id'`.

`{{ this }}` is a Jinja shortcut for "this model's own warehouse name" — used to look up the current high-water mark.

**When to use:**
- Big fact tables (events, orders, page views) where rebuilding the whole thing is wasteful.
- Append-only or merge-able streams.

**When NOT to use:**
- Tables where every row changes every day (a full rebuild may be cheaper than a merge).
- Tables small enough to rebuild in seconds.

(Full deep-dive in `09_incremental_deep_dive.md` — including `merge` vs `delete+insert` vs `append` strategies, `on_schema_change`, and `--full-refresh`.)

---

## **5. LINE-BY-LINE: A REAL `view` MODEL**

```sql
-- models/staging/stg_customers.sql

{{ config(
    materialized='view',
    schema='staging'
) }}                                      -- Line A

with source as (                          -- Line B
    select * from {{ source('shop_raw', 'customers') }}   -- Line C
)

select                                    -- Line D
    customer_id::int     as customer_id,
    initcap(trim(name))  as name,
    lower(trim(email))   as email,
    created_at::timestamp as created_at
from source
```

**What each line does and what's produced:**

- **Line A:** Compile-time config. `materialized='view'` makes dbt issue `CREATE OR REPLACE VIEW`. `schema='staging'` overrides the default schema → the view is built as `<database>.<schema>_staging.stg_customers` (dbt prefixes the schema by default to avoid collisions; configurable).
- **Line B–C:** Standard SQL CTE pattern. `{{ source('shop_raw', 'customers') }}` resolves at compile to the fully-qualified raw table name — covered in file 03. After compilation: `SELECT * FROM raw_db.shop_raw.customers`.
- **Line D onward:** Type casts and string normalization. The model output schema is fixed by these column expressions.

**What ends up in the warehouse:**

```sql
CREATE OR REPLACE VIEW shop.main_staging.stg_customers AS (
    with source as ( select * from raw_db.shop_raw.customers )
    select
        customer_id::int     as customer_id,
        initcap(trim(name))  as name,
        lower(trim(email))   as email,
        created_at::timestamp as created_at
    from source
);
```

The view is cheap to build (no data copy), but every read recomputes the trim/initcap/lower. Acceptable for staging because staging models are usually consumed by 1–2 downstream models that themselves materialize.

---

## **6. CHANGING MATERIALIZATION — IT'S A CONFIG**

You can change a materialization in three places, in order of specificity (more specific wins):

### **6.1 In the model file itself (most specific)**

```sql
{{ config(materialized='table') }}
select ...
```

### **6.2 In a YAML schema file**

```yaml
# models/marts/_marts.yml
version: 2
models:
  - name: customer_orders
    config:
      materialized: table
```

### **6.3 In `dbt_project.yml` (least specific, broadest)**

```yaml
models:
  shop_dbt:
    marts:
      +materialized: table       # all models under marts/ default to table
      core:
        +materialized: incremental  # models under marts/core/ override to incremental
```

**The `+` prefix** marks "this is a config" so dbt doesn't confuse it with a folder name. Without `+`, dbt would look for a folder literally called `materialized`.

---

## **7. BUILD ALONG**

### **Step 1.** In your `shop_dbt` project, add `models/customers_view.sql`:

```sql
{{ config(materialized='view') }}
select customer_id, upper(name) as name_upper from {{ ref('customers') }}
```

### **Step 2.** Add `models/customers_table.sql`:

```sql
{{ config(materialized='table') }}
select customer_id, upper(name) as name_upper from {{ ref('customers') }}
```

### **Step 3.** Run them:

```bash
dbt run --select customers_view customers_table
```

### **Step 4.** Confirm what was created:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb \
  "SELECT table_name, table_type FROM information_schema.tables WHERE table_name LIKE 'customers_%'"
```

**Expected output:**

```
┌──────────────────┬────────────┐
│   table_name     │ table_type │
├──────────────────┼────────────┤
│ customers_table  │ BASE TABLE │
│ customers_view   │ VIEW       │
└──────────────────┴────────────┘
```

Same SELECT, different physical objects.

### **Step 5.** Switch one to ephemeral. Edit `customers_view.sql`:

```sql
{{ config(materialized='ephemeral') }}
select customer_id, upper(name) as name_upper from {{ ref('customers') }}
```

### **Step 6.** Re-run:

```bash
dbt run --select customers_view
```

You'll see `customers_view` is **skipped** — ephemeral models don't run on their own. Confirm it's gone from the warehouse:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb "SELECT table_name FROM information_schema.tables WHERE table_name='customers_view'"
```

Empty result. The ephemeral model now exists only as a CTE that gets inlined into anything `ref()`-ing it.

---

## **8. REAL-WORLD USE CASES**

| Layer | Typical materialization | Why |
|---|---|---|
| `stg_*` (staging) | `view` | Cheap to rebuild, light transforms only, always fresh |
| `int_*` (intermediate) | `view` or `ephemeral` | Logical helpers, rarely consumed by BI directly |
| `dim_*` (dimensions) | `table` | Stable, joined a lot, want fast reads |
| `fct_*` (facts, big) | `incremental` | Append-mostly, rebuilding is wasteful |
| `fct_*` (facts, small) | `table` | Small enough to rebuild fast; simpler than incremental |
| `metric_*` / aggregates | `table` | Heavy aggregation, want fast BI reads |

In production at Fortune-500 scale, the breakdown is roughly: staging ~70% views, intermediate ~50/50, marts ~70% tables (with the largest 5–10% being incremental).

---

## **9. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Default to `view`** in `dbt_project.yml`. Escalate to `table` only when you can articulate why.
- **Configure at the folder level**, not in every file. Use `{{ config(...) }}` in a model only when overriding the default.
- **Reserve `ephemeral` for one-shot helpers.** If you find yourself with 3+ refs to one ephemeral model, convert it to a view.
- **Never make staging incremental** — staging should be deterministic and cheap.

### **Anti-patterns**

- **`materialized='table'` everywhere** because "tables are faster." You're paying compute on every run for queries that may rarely be hit.
- **Long ephemeral chains** (5+ ephemerals in a row) — produces unreadable compiled SQL with deeply nested CTEs.
- **Incremental without thinking about uniqueness** — leads to silent duplication. `unique_key` is mandatory in nearly every real incremental.
- **Using a model name that already exists in the warehouse** as a non-dbt object. dbt will overwrite it. Always use a dedicated schema for dbt-managed objects.

---

## **10. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What are the four built-in dbt materializations?**

**Model answer:** `view`, `table`, `ephemeral`, and `incremental`. `view` and `table` map to `CREATE OR REPLACE VIEW/TABLE` DDL. `ephemeral` doesn't physicalize — it's inlined as a CTE in downstream models. `incremental` builds the full table on first run, then on subsequent runs only processes new rows and merges them in.

### **Q2. [Foundational] How would you choose between view and table?**

**Model answer:** A view is cheap to build but expensive on every read because it recomputes the SELECT. A table is expensive to build (writes data) but cheap to read. So if a model is consumed many times between runs (a typical BI table), table wins. If a model is light, fresh-on-every-read, or barely queried, view wins. The dollar math: table cost = compute(build) once per run. View cost = compute(SELECT) × reads. The break-even depends on read frequency.

### **Q3. [Intermediate] When does an ephemeral model break down?**

**Model answer:** When it's referenced by many downstream models or has heavy aggregation. dbt inlines an ephemeral as a CTE in every downstream model that refs it, so a single ephemeral becomes copy-pasted into N compiled SQL queries. The warehouse plans them independently — so you pay the ephemeral's cost N times instead of once. Three signs to convert to view: (1) 3+ downstream refs, (2) aggregations, (3) joins to large tables. Also, error messages on ephemeral tests are confusing because they surface in the downstream model.

### **Q4. [Intermediate] What does `is_incremental()` return, and when?**

**Model answer:** `is_incremental()` is a Jinja macro that returns true only when **all** of these are true: (1) the model's materialization is `incremental`, (2) the target table already exists in the warehouse, (3) `--full-refresh` was NOT passed. So on the first ever run it returns false (target doesn't exist), letting the model's full SELECT build the table. On subsequent runs it returns true, activating the WHERE-filter that limits to new rows. On `--full-refresh` it returns false, forcing a rebuild.

### **Q5. [Advanced] How do dbt materializations work internally? Could you implement one?**

**Model answer:** Materializations are macros in dbt-core's source. Each one is a Jinja+SQL block wrapped in `{% materialization name, default %} … {% endmaterialization %}`. The block has access to a few primitives: `{{ sql }}` (the model's compiled SELECT), `{{ this }}` (the target relation), `adapter.rename_relation`, `adapter.drop_relation`, etc. You can override or define your own by putting a `{% materialization %}` block in a macro file. Custom materializations are rarely needed — the built-in four cover ~95% of use cases — but dbt's docs show how to write one. The internal implementations live in `dbt-core/dbt/include/global_project/macros/materializations/`.

---

## **11. GOTCHAS**

- **Schema configuration.** If you set `schema: staging` in a model's config, the warehouse schema becomes `<target_schema>_staging`, not literally `staging`. dbt prefixes to avoid collisions across targets. Configurable via `generate_schema_name` macro.
- **Switching materialization.** Going `view → table` means the next `dbt run` drops the view and creates a table in its place. Going `table → incremental` works on first run, but `incremental → table` requires `--full-refresh`.
- **`OR REPLACE` semantics.** Some warehouses don't support `CREATE OR REPLACE` atomically. dbt emulates with a temp-table swap. Brief windows of inconsistency are possible — use Delta Lake / Snowflake / BigQuery, where atomic swap is native.
- **Ephemeral models are silently invisible to BI.** If a stakeholder asks "where's `customers_clean`?" and it's ephemeral, the answer is "it doesn't exist as a warehouse object."
- **`ref()` to ephemeral works.** `ref()` to a non-dbt-managed table doesn't. Use `source()` for that.
- **Materializations are warehouse-aware.** `table` on Snowflake uses `CREATE OR REPLACE TABLE`. On Redshift it's `CREATE TABLE` + `RENAME` because Redshift lacks `CREATE OR REPLACE`. The adapter handles the difference.

---

## **NEXT STEP**

You can run models. Now learn how to wire them together — `ref()` and `source()`.

Go to [`03_sources_and_ref.md`](03_sources_and_ref.md).
