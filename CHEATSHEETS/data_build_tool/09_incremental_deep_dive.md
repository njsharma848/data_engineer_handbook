# **09 — Incremental Models, Deep Dive**

> **Goal:** Master the production-critical materialization. Cover all incremental strategies (`merge`, `delete+insert`, `append`, `insert_overwrite`), `unique_key` semantics, `on_schema_change`, late-arriving data, idempotency, `--full-refresh`, and the failure modes that break interviews.

---

## **1. THE MENTAL MODEL**

An incremental model is a `table`-materialized model that, after the first build, **only processes new (or changed) rows** rather than rebuilding the whole table.

```
First run:
    raw_orders ──┐
                 ├── CREATE TABLE main.fct_orders AS (SELECT * FROM raw_orders)
                 │   (full table built)
                 └

Subsequent runs:
    raw_orders ──┐
                 ├── 1. Build temp table of NEW rows only
                 │      (filtered with `where order_date > max(order_date) in {{ this }}`)
                 ├── 2. MERGE temp INTO fct_orders ON unique_key
                 │      (insert new + update changed)
                 └── 3. Drop temp
```

The whole point is to avoid rebuilding the entire table when most of it didn't change. That saves enormous compute on big fact tables.

---

## **2. WHY IT EXISTS**

A 1-billion-row order fact table on Snowflake costs ~$5–20 per full rebuild. Run it hourly = $120–500/day. Run it incrementally = $5–20/day. That's the math that drove incremental adoption.

The trade-off: incremental models add complexity. You now have:

- A high-water mark to manage.
- Late-arriving data to handle.
- A merge strategy to choose.
- Schema evolution to plan for.
- A `--full-refresh` escape hatch to remember.

For most models, simple `table` materialization is fine. For the largest 5–10% of models in a project, incremental is the right tool. Don't reach for it prematurely.

---

## **3. THE MINIMAL INCREMENTAL — REVISITED**

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select * from {{ ref('stg_shop__orders') }}

{% if is_incremental() %}
  where order_date > (select coalesce(max(order_date), '1900-01-01') from {{ this }})
{% endif %}
```

Three things to note:

1. **`is_incremental()`** is true only on the second-and-later run, when the target exists and `--full-refresh` wasn't passed.
2. **`{{ this }}`** is the model's own warehouse name. Used to look up the existing high-water mark.
3. **`coalesce(max(order_date), '1900-01-01')`** handles the edge case where the table exists but is empty.

What dbt does on each run:

| Run | `is_incremental()` | What runs | Result |
|---|---|---|---|
| First | `false` | Full SELECT, `CREATE TABLE AS …` | Table built from scratch |
| Second | `true` | Filtered SELECT (new rows only), `MERGE` into target | Only new rows added/updated |
| Nth, with `--full-refresh` | `false` | Full SELECT, `CREATE OR REPLACE TABLE AS …` | Table rebuilt |

---

## **4. THE FOUR INCREMENTAL STRATEGIES**

dbt's incremental materialization supports multiple **strategies** for "how do I merge the new rows into the target." Different warehouses support different sets.

| Strategy | What it does | Adapter support |
|---|---|---|
| `merge` | Real `MERGE` SQL (insert new, update existing) | Snowflake, BigQuery, Databricks, DuckDB, Postgres ≥ 15 |
| `delete+insert` | Delete by unique_key, then insert new rows | Postgres, Redshift, others without MERGE |
| `append` | Always insert; never update | All adapters |
| `insert_overwrite` | Replace partitions atomically | BigQuery, Spark/Databricks |
| `microbatch` (1.9+) | Time-bounded micro-batches | Most adapters |

Configure with `incremental_strategy='merge'` (or whichever).

### **4.1 `merge`**

The default on most modern warehouses. dbt generates:

```sql
MERGE INTO main.fct_orders AS target
USING (<your filtered SELECT>) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

Best when:
- Rows can be both new (insert) and changed (update).
- Warehouse supports atomic MERGE.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id'
) }}
```

### **4.2 `delete+insert`**

Two-step, used when MERGE isn't supported (older Postgres, Redshift):

```sql
DELETE FROM main.fct_orders WHERE order_id IN (SELECT order_id FROM <new>);
INSERT INTO main.fct_orders SELECT * FROM <new>;
```

Best when MERGE unavailable. Slightly slower than MERGE; not atomic without explicit transaction.

### **4.3 `append`**

Pure insert, no uniqueness check:

```sql
INSERT INTO main.fct_orders SELECT * FROM <new>;
```

Best when:
- Append-only data (clickstream events, logs).
- You're 100% sure no duplicates can appear.
- You don't need updates.

**Risk:** if the same row arrives twice (re-ingestion, source replay), you get duplicates. Use only with idempotency guarantees upstream.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}
```

No `unique_key` needed (and ignored if provided).

### **4.4 `insert_overwrite`**

Partition-replace strategy:

```sql
-- Logically:
DELETE FROM main.fct_orders WHERE partition_date IN (<partitions in new data>);
INSERT INTO main.fct_orders SELECT * FROM <new>;
```

Best for:
- Time-partitioned tables where you want to reprocess full partitions.
- BigQuery / Spark, where partition replace is atomic and fast.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'order_date', 'data_type': 'date'}
) }}
```

Requires the partition column to be configured. dbt overwrites the partitions present in the new data — good for late-arriving updates within a partition.

### **4.5 `microbatch` (dbt 1.9+)**

A newer strategy that breaks the incremental window into time-bounded micro-batches and processes each independently. Fixes the "what if today's run failed and we need to backfill yesterday plus today?" problem cleanly.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='order_at',
    batch_size='day',
    lookback=2,
    begin='2025-01-01'
) }}

select * from {{ ref('stg_shop__orders') }}
```

dbt automatically generates the `where order_at >= ... and order_at < ...` predicates for each micro-batch and runs them in sequence (or parallel, depending on config). Late-arriving data within `lookback` is reprocessed on every run.

---

## **5. `unique_key` SEMANTICS**

`unique_key` is **not** a constraint — the warehouse doesn't enforce it. It's the **join key** dbt uses for `MERGE` / `delete+insert`.

### **5.1 Single-column key**

```sql
{{ config(materialized='incremental', unique_key='order_id') }}
```

If two rows have the same `order_id`, MERGE picks one to UPDATE — typically nondeterministic. Always test for uniqueness:

```yaml
- name: order_id
  data_tests: [unique]
```

### **5.2 Composite key**

```sql
{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id']
) }}
```

A list. dbt generates `target.order_id = source.order_id AND target.product_id = source.product_id` in the MERGE.

### **5.3 No `unique_key` (append-only)**

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}
```

dbt skips dedup logic entirely. Use with caution — duplicates are silent.

### **5.4 `unique_key` with merge but no actual uniqueness**

This is a common bug. If `unique_key='customer_id'` but multiple rows per customer exist in the source, MERGE will only update one row per customer per run. The other rows aren't merged correctly. Fix: use a composite key that's actually unique, or pre-aggregate before the incremental.

---

## **6. LATE-ARRIVING DATA — THE BIGGEST FOOTGUN**

The naive incremental:

```sql
where order_date > (select max(order_date) from {{ this }})
```

**Bug:** if a row with `order_date = '2026-01-10'` arrives on `2026-01-15` (after the high-water mark advanced past it), the predicate excludes it forever. Lost row.

### **6.1 Solution 1: lookback window**

```sql
where order_date >= (select dateadd('day', -3, max(order_date)) from {{ this }})
```

Reprocess the last 3 days every run. With MERGE + correct `unique_key`, late-arriving rows get inserted; existing rows might get re-updated harmlessly (if your transformations are deterministic).

### **6.2 Solution 2: `microbatch` (dbt 1.9+)**

`microbatch` strategy with `lookback=3` does this declaratively:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='order_at',
    batch_size='day',
    lookback=3
) }}
```

dbt re-runs the last 3 days of micro-batches every run. No manual high-water-mark logic.

### **6.3 Solution 3: ingested_at, not event_time**

If you have `_ingested_at` (when the row was loaded) vs `order_date` (when the order happened), filter on `_ingested_at`:

```sql
where _ingested_at > (select max(_ingested_at) from {{ this }})
```

This catches every row regardless of event time. Simple and correct.

---

## **7. `on_schema_change`**

What happens when you add a column to an incremental model and run it? Default behavior: dbt errors because the new SELECT has columns the existing table doesn't have.

`on_schema_change` controls this:

| Value | Behavior |
|---|---|
| `ignore` (default in older dbt) | Error on schema change. Forces explicit decision. |
| `fail` | Same as ignore. Loud error. |
| `append_new_columns` | New columns are added to the target with NULL for historical rows. |
| `sync_all_columns` | Add new, drop removed. Risky — drops can lose data. |

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}
```

For most projects: `append_new_columns` is the right default. Adding columns is common; dropping should be deliberate.

---

## **8. `--full-refresh`**

`--full-refresh` tells dbt to ignore `is_incremental()` and rebuild the table from scratch.

```bash
dbt run --select fct_orders --full-refresh
```

When to use:
- After a schema change you want to backfill (e.g., new column, populate historical rows).
- After a logic change in the SELECT (the existing table has the old logic).
- Periodic "ground truth" rebuild to fix drift from append/merge bugs.

Production caveat: `--full-refresh` can be expensive. Some shops disable it via `+full_refresh: false` config to prevent accidents on huge tables.

```yaml
# dbt_project.yml — protect production from full-refresh
models:
  shop_dbt:
    marts:
      core:
        +full_refresh: false   # `--full-refresh` is ignored
```

The model can only be rebuilt by manually dropping the table.

---

## **9. WORKED EXAMPLE — A REAL INCREMENTAL**

```sql
-- models/marts/core/fct_orders.sql

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='append_new_columns',
    cluster_by=['order_date'],          -- BigQuery / Snowflake
    tags=['nightly', 'core']
) }}

with new_orders as (

    select * from {{ ref('stg_shop__orders') }}

    {% if is_incremental() %}
      -- Lookback 3 days for late-arriving data
      where order_date >= (
          select coalesce(dateadd('day', -3, max(order_date)), '1900-01-01')
          from {{ this }}
      )
    {% endif %}

),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.amount,
        o.status,
        c.tier as customer_tier,
        current_timestamp as _dbt_loaded_at
    from new_orders o
    left join {{ ref('dim_customers') }} c using (customer_id)
)

select * from enriched
```

**Line-by-line:**

- `{{ config(...) }}`: incremental, merge, `unique_key='order_id'`, schema-evolution-friendly, clustered by date (warehouse-specific perf hint), tagged for selectors.
- `new_orders` CTE: starts from staging.
- `{% if is_incremental() %}`: guard. On the first run this block is skipped. On subsequent runs it adds the WHERE filter.
- `dateadd('day', -3, max(order_date))`: 3-day lookback for late-arriving data. Wrapped in `coalesce` for the empty-table edge case.
- `enriched` CTE: joins to a dim. The left join is full because the dim is small — the cost is negligible compared to scanning all of `stg_shop__orders`.
- `_dbt_loaded_at`: an audit column to know when each row was last touched.

What runs on first invocation:

```sql
CREATE OR REPLACE TABLE main.fct_orders AS (
    -- entire SELECT, no WHERE filter
    select ... from stg_shop__orders
);
```

What runs on subsequent invocations:

```sql
CREATE OR REPLACE TEMPORARY TABLE _fct_orders_tmp_xyz AS (
    select ... from stg_shop__orders
    where order_date >= dateadd('day', -3, (select max(order_date) from main.fct_orders))
);

MERGE INTO main.fct_orders AS target
USING _fct_orders_tmp_xyz AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET <all columns>
WHEN NOT MATCHED THEN INSERT (<columns>) VALUES (<values>);
```

---

## **10. BUILD ALONG — INCREMENTAL ON DUCKDB**

### **Step 1.** Set up a "raw" log of orders that we'll grow over time:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb <<'EOF'
CREATE OR REPLACE TABLE raw.orders_log AS
  SELECT * FROM (VALUES
    (1, 1, '2026-01-01', 50.00, 'shipped'),
    (2, 1, '2026-01-02', 75.00, 'shipped'),
    (3, 2, '2026-01-03', 30.00, 'pending')
  ) AS t(order_id, customer_id, order_date, amount, status);
EOF
```

### **Step 2.** Add the source declaration in `_sources.yml`:

```yaml
sources:
  - name: shop_raw
    database: shop
    schema: raw
    tables:
      - name: orders_log
```

### **Step 3.** Create the incremental model `models/marts/core/fct_orders_incremental.sql`:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

select
    order_id,
    customer_id,
    order_date::date as order_date,
    amount,
    status,
    current_timestamp as _dbt_loaded_at
from {{ source('shop_raw', 'orders_log') }}

{% if is_incremental() %}
  where order_date > (select coalesce(max(order_date), '1900-01-01') from {{ this }})
{% endif %}
```

### **Step 4.** Run for the first time:

```bash
dbt run --select fct_orders_incremental
```

Expected:

```
1 of 1 START sql incremental model main.fct_orders_incremental [RUN]
1 of 1 OK created sql incremental model main.fct_orders_incremental [INSERT 0, INSERT 3 in ...s]
```

### **Step 5.** Verify:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb \
  "SELECT order_id, order_date, _dbt_loaded_at FROM main.fct_orders_incremental ORDER BY order_id"
```

Three rows.

### **Step 6.** Add new rows to source:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb <<'EOF'
INSERT INTO raw.orders_log VALUES
  (4, 3, '2026-01-04', 100.00, 'shipped'),
  (5, 1, '2026-01-05', 25.00, 'shipped');
EOF
```

### **Step 7.** Run again:

```bash
dbt run --select fct_orders_incremental
```

Expected:

```
1 of 1 START sql incremental model main.fct_orders_incremental [RUN]
1 of 1 OK created sql incremental model main.fct_orders_incremental [INSERT 0 1, INSERT 0 1 in ...s]
```

dbt only inserted 2 rows (the new ones). Verify:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb \
  "SELECT order_id, _dbt_loaded_at FROM main.fct_orders_incremental ORDER BY order_id"
```

The first 3 rows have an older `_dbt_loaded_at`; the new 2 rows have a newer timestamp. **The model only processed new rows.**

### **Step 8.** Test `--full-refresh`:

```bash
dbt run --select fct_orders_incremental --full-refresh
duckdb /tmp/dbt_workspace/shop.duckdb \
  "SELECT order_id, _dbt_loaded_at FROM main.fct_orders_incremental ORDER BY order_id"
```

All rows now have the same `_dbt_loaded_at` — full rebuild.

### **Step 9.** Test merge update behavior:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb \
  "UPDATE raw.orders_log SET amount = 999.99 WHERE order_id = 4"

dbt run --select fct_orders_incremental
```

The current SELECT filters with `order_date > max(order_date)` — order 4's date isn't past the watermark, so this update **isn't picked up** (footgun!). To pick up updates, you'd need a different strategy: filter on `_ingested_at` or use `microbatch`.

This is the canonical late-arriving / mutation problem. File 11 covers detection in CI.

---

## **11. REAL-WORLD USE CASES**

- **Clickstream events** (1B+ rows/day) → `incremental` + `append` + filtered by `_ingested_at`. No updates expected.
- **Order facts** with status changes → `incremental` + `merge` on `order_id`, with lookback for late updates.
- **Time-series rollups** (hourly aggregates) → `insert_overwrite` on partition; reprocess full hours when source data shifts.
- **Stripe payments** → `merge` on `charge_id`. Charges go from `pending` → `succeeded` over time; merge handles the update cleanly.
- **CDC streams from Debezium** → `merge` on PK with a deleted-at filter.

---

## **12. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Always test `unique` on `unique_key`.** Catches the silent-dedup bug.
- **Always include a lookback** unless you have airtight idempotency upstream.
- **Use `_ingested_at` as the high-water mark column** when possible. Less foot-gun than `event_date`.
- **Add a `_dbt_loaded_at` audit column** on every incremental. Massive debugging help.
- **Use `microbatch` (dbt 1.9+)** for any new incremental that's time-partitioned. Cleaner than DIY.
- **Set `+full_refresh: false`** on truly huge tables in prod to prevent accidents.
- **`on_schema_change: append_new_columns`** as the default.

### **Anti-patterns**

- **No `unique_key` on `merge` strategy.** dbt errors, but if you swap to `append` to "fix" it, you've just chosen silent duplicates.
- **`unique_key` that isn't actually unique.** MERGE picks one row to UPDATE nondeterministically.
- **Naive high-water mark with no lookback.** Late-arriving data lost forever.
- **`--full-refresh` in production cron** as the "easy fix." Either fix the bug or accept the cost — don't paper over it.
- **Incremental on a model that takes <30s to fully rebuild.** Complexity not worth it.
- **Mixing strategies across runs** (one PR sets `merge`, the next sets `delete+insert`) — leads to confused state.
- **Forgetting to handle the empty-table edge case.** `max(order_date)` of an empty table is NULL → predicate filters out everything → empty SELECT → no rows inserted. Wrap with `coalesce`.

---

## **13. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What does `is_incremental()` return and when?**

**Model answer:** `is_incremental()` returns `true` only when all three are true: (1) the model's materialization is `incremental`, (2) the target table already exists in the warehouse, (3) `--full-refresh` was not passed. So on the very first run it returns `false` (target doesn't exist), letting dbt build the full table. On subsequent runs it returns `true`, activating the incremental WHERE filter. With `--full-refresh` it returns `false`, forcing a rebuild from scratch.

### **Q2. [Foundational] What's the difference between `merge`, `delete+insert`, and `append` strategies?**

**Model answer:** `merge` uses a real `MERGE` SQL statement to insert new rows and update existing ones — atomic, fast on warehouses that support it (Snowflake, BigQuery, Databricks, Postgres ≥15). `delete+insert` does the same logically but in two steps — used on warehouses without MERGE (Redshift). `append` only inserts; it never updates and skips dedup logic — used for append-only event streams where idempotency is guaranteed upstream. `merge` is the default and right answer most of the time; `append` is correct for clickstream / log-style data.

### **Q3. [Intermediate] How do you handle late-arriving data in an incremental model?**

**Model answer:** The naive `where event_date > max(event_date)` loses any row whose `event_date` is older than the max but arrives after the run. Fixes: (1) use a lookback window — `where event_date >= dateadd('day', -3, max(event_date))` — and rely on MERGE to dedup; (2) filter on `_ingested_at` (when loaded) instead of `event_date` (when happened); (3) use `microbatch` strategy in dbt 1.9+ with `lookback=N`, which automates the lookback. Whatever the choice, the `unique_key` must actually be unique or MERGE will misbehave.

### **Q4. [Intermediate] What's `on_schema_change` and why does it matter?**

**Model answer:** When you add a column to an incremental model's SELECT and re-run, the new column doesn't exist in the target table — dbt errors by default. `on_schema_change` controls how dbt reacts: `fail`/`ignore` errors out, `append_new_columns` adds new columns with NULL for historical rows, `sync_all_columns` adds new and drops removed. `append_new_columns` is the right default for most projects: it gracefully handles the common case (adding a column) without risking data loss (dropping). Production teams set this in `dbt_project.yml` so every incremental gets the same behavior.

### **Q5. [Advanced] Walk me through what dbt does internally on a `merge`-strategy incremental run.**

**Model answer:** Three steps:

1. **Build a temp table** with the filtered SELECT (`where _ingested_at > max(...)`). This is the "delta."
2. **Issue a `MERGE INTO target USING temp ON unique_key`** with `WHEN MATCHED THEN UPDATE SET <all cols>` and `WHEN NOT MATCHED THEN INSERT`. The MERGE is atomic on warehouses that support it.
3. **Drop the temp table**.

The temp table approach exists so the SELECT (which can include heavy joins/CTEs) runs once and the warehouse can plan the MERGE against a known relation. dbt also writes the rendered MERGE to `target/run/.../<model>.sql` so you can inspect it.

### **Q6. [Advanced] Your incremental model is producing duplicates. What's likely wrong, and how would you debug?**

**Model answer:** Three common causes:

1. **`unique_key` isn't actually unique** — multiple rows per key in the source. The MERGE picks one nondeterministically; the others are inserted as new rows. Fix: add `unique` test, find the real composite key, or pre-aggregate.
2. **`incremental_strategy='append'` with non-idempotent ingestion** — the same source row appears twice across runs. Fix: switch to `merge` with a real `unique_key`, or filter on `_ingested_at` to ensure idempotency.
3. **Schema change with `sync_all_columns`** that dropped the unique key column. Fix: never drop the unique_key.

To debug: `select unique_key, count(*) from {{ this }} group by 1 having count(*) > 1`. Then look at the offending rows' `_dbt_loaded_at` to find when the dup was inserted, and trace back to that run's input.

### **Q7. [Advanced] When would you use `insert_overwrite` instead of `merge`?**

**Model answer:** When the table is partitioned and you want to **replace whole partitions atomically**. `merge` updates rows individually and may be slower or unsupported in some scenarios (Spark/Hive, partitioned BigQuery). `insert_overwrite` deletes the partitions present in the new data and inserts fresh — atomic, fast, and conceptually simpler when "the right state for partition X is whatever the new SELECT produces." Common use cases: hourly/daily aggregates where you reprocess full days. Trade-off: you lose row-level dedup; if the new SELECT is missing a row, that row is gone after the overwrite.

---

## **14. GOTCHAS**

- **Empty table on first run.** `max(order_date)` is NULL → WHERE filter excludes everything → 0 rows inserted. Always `coalesce(max(order_date), '<sentinel>')`.
- **`unique_key` is not enforced.** dbt uses it as a join key; the warehouse doesn't reject duplicates. Test it.
- **MERGE and column order.** If the new SELECT has columns in a different order than the existing table, MERGE may silently swap columns. Always SELECT named columns, never `SELECT *` directly into a target whose order may differ.
- **`is_incremental()` returns false during `dbt parse`** — don't use it in macros that run at parse.
- **`--full-refresh` rebuilds even if the SELECT is a one-line tweak.** Use it deliberately.
- **`{{ this }}` doesn't work outside incremental.** It's defined for any model, but referencing your own table is meaningless except inside the `is_incremental()` block.
- **Late-arriving updates** to a row whose `event_date` is past the lookback window are silently lost. Either widen the lookback or switch the high-water-mark to `_ingested_at`.
- **`microbatch` strategy** (1.9+) is not yet supported by every adapter. Check your adapter docs.
- **`unique_key` as a list** — older dbt versions only accepted a string. Modern accepts a list. If your CI uses an old dbt, lists may fail silently.
- **Schema evolution + `sync_all_columns`** can drop a column populated by another team's pipeline. Use `append_new_columns` instead.

---

## **NEXT STEP**

You can build incremental models. Now learn the surrounding production scaffolding — hooks, vars, profiles.

Go to [`10_hooks_vars_configs.md`](10_hooks_vars_configs.md).
