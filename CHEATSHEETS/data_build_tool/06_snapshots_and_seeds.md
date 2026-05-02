# **06 — Snapshots and Seeds**

> **Goal:** Implement Slowly Changing Dimensions Type 2 with `dbt snapshot`, and load static reference data with `dbt seed`. Both are first-class node types in the DAG that complement models.

---

## **1. THE TWO PROBLEMS**

dbt models are stateless: every run rebuilds (or merges into) the target. If your source table mutates *in place* — a customer's `tier` changes from Bronze to Gold — you lose the old value forever. Snapshots fix that by capturing point-in-time history. SCD Type 2.

Seeds solve a different problem: small reference tables (country codes, status mappings, FX rates) that belong in the repo, not in your warehouse's ingestion path.

---

## **2. SNAPSHOTS**

### **2.1 Mental Model**

A snapshot is a **table dbt maintains** that captures every change to a source row over time. Each row gets `dbt_valid_from` / `dbt_valid_to` timestamps, so you can query "what was customer 42's tier on 2025-08-15?"

Conceptually:

```
Source (mutates in place)
   id=1, tier='Bronze', updated_at=2025-01-01
       ↓ source row updated to Gold on 2025-08-15
   id=1, tier='Gold',   updated_at=2025-08-15

Snapshot (full history)
   id=1, tier='Bronze', dbt_valid_from=2025-01-01, dbt_valid_to=2025-08-15
   id=1, tier='Gold',   dbt_valid_from=2025-08-15, dbt_valid_to=NULL    ← current
```

A query for "active record" filters to `dbt_valid_to is null`. A query for "tier on date X" filters to `dbt_valid_from <= X < coalesce(dbt_valid_to, current_date+1)`.

### **2.2 Why It Exists**

Without snapshots, you'd hand-roll SCD2 with a `merge` statement that compares `current_data` to the prior snapshot, expires changed rows, inserts new versions. Doable but error-prone — and every analyst on the team has to re-implement it. dbt ships a snapshot primitive that handles the merge logic, exposes the SCD2 columns, and runs as part of `dbt build`.

### **2.3 How It Works Under the Hood**

A snapshot defines a SELECT that returns the current state of the source. dbt stores the snapshot as a regular table in the warehouse with extra columns. On each `dbt snapshot` run:

1. Run the SELECT to get current source state.
2. Compare each row's hash (or `updated_at` column) to the existing snapshot row.
3. For changed rows: update the existing row's `dbt_valid_to = now()` (expire it), and INSERT a new row with `dbt_valid_from = now()`, `dbt_valid_to = NULL`.
4. For brand-new rows: INSERT with `dbt_valid_from = now()`, `dbt_valid_to = NULL`.
5. For deleted rows (if `invalidate_hard_deletes=true`): set `dbt_valid_to = now()`.

The four maintained columns:

| Column | Meaning |
|---|---|
| `dbt_scd_id` | Surrogate key — hash of unique_key + dbt_valid_from |
| `dbt_updated_at` | When this row was last detected as new/changed |
| `dbt_valid_from` | Start of this row's validity window |
| `dbt_valid_to` | End of validity (NULL = still current) |

### **2.4 Two Strategies — `timestamp` vs `check`**

#### `timestamp` strategy (cheaper, recommended)

Pick a column that increments whenever the row changes (e.g., `updated_at`). dbt detects changes by comparing this single column.

```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('shop_raw', 'customers') }}

{% endsnapshot %}
```

#### `check` strategy (no reliable updated_at)

dbt computes a hash of the listed columns each run and compares to the prior hash.

```jinja
{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=['name', 'email', 'tier'],
    )
}}
```

`check_cols=['*']` means hash every column except `unique_key`. Slower but works when source has no `updated_at`.

### **2.5 Snapshot config keys**

| Key | Required | Meaning |
|---|---|---|
| `target_schema` | ✅ | Schema where the snapshot table lives |
| `unique_key` | ✅ | The primary key of the source row (a single column or a Jinja-rendered list) |
| `strategy` | ✅ | `timestamp` or `check` |
| `updated_at` | If `timestamp` | The source's update-detection column |
| `check_cols` | If `check` | List of columns to hash |
| `invalidate_hard_deletes` | optional | Default `false`. Set `true` to expire rows that disappear from source |
| `target_database` | optional | Override DB |

### **2.6 SCD Type 2 in 50 Lines — The Build Along**

#### Step 1. Set up a "raw" source that we'll mutate.

```bash
duckdb /tmp/dbt_workspace/shop.duckdb <<'EOF'
CREATE OR REPLACE TABLE raw.customer_tiers AS
  SELECT * FROM (VALUES
    (1, 'Alice', 'Bronze', TIMESTAMP '2025-01-01 09:00:00'),
    (2, 'Bob',   'Bronze', TIMESTAMP '2025-01-01 09:00:00'),
    (3, 'Carol', 'Silver', TIMESTAMP '2025-01-01 09:00:00')
  ) AS t(id, name, tier, updated_at);
EOF
```

#### Step 2. Add the snapshot file: `snapshots/customer_tiers_snap.sql`

```sql
{% snapshot customer_tiers_snap %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select id, name, tier, updated_at
from {{ source('shop_raw', 'customer_tiers') }}

{% endsnapshot %}
```

#### Step 3. Declare the source in `_sources.yml`:

```yaml
sources:
  - name: shop_raw
    database: shop
    schema: raw
    tables:
      - name: customer_tiers
```

#### Step 4. Run for the first time:

```bash
dbt snapshot
```

Output:

```
Found 0 models, 1 snapshot, ...

1 of 1 START snapshot snapshots.customer_tiers_snap [RUN]
1 of 1 OK snapshotted snapshots.customer_tiers_snap [OK in ...s]
```

dbt created the snapshot table with three rows and four added columns.

#### Step 5. Mutate the source:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb <<'EOF'
UPDATE raw.customer_tiers
SET tier = 'Gold', updated_at = TIMESTAMP '2026-04-15 10:00:00'
WHERE id = 1;

INSERT INTO raw.customer_tiers VALUES (4, 'Dan', 'Bronze', TIMESTAMP '2026-04-15 10:00:00');
EOF
```

#### Step 6. Run the snapshot again:

```bash
dbt snapshot
```

#### Step 7. Inspect history:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb \
  "SELECT id, name, tier, dbt_valid_from, dbt_valid_to FROM snapshots.customer_tiers_snap ORDER BY id, dbt_valid_from"
```

Expected:

```
┌────┬───────┬────────┬──────────────────────┬──────────────────────┐
│ id │ name  │  tier  │  dbt_valid_from      │   dbt_valid_to       │
├────┼───────┼────────┼──────────────────────┼──────────────────────┤
│ 1  │ Alice │ Bronze │ 2025-01-01 09:00:00  │ 2026-04-15 10:00:00  │ ← expired
│ 1  │ Alice │ Gold   │ 2026-04-15 10:00:00  │ NULL                 │ ← current
│ 2  │ Bob   │ Bronze │ 2025-01-01 09:00:00  │ NULL                 │
│ 3  │ Carol │ Silver │ 2025-01-01 09:00:00  │ NULL                 │
│ 4  │ Dan   │ Bronze │ 2026-04-15 10:00:00  │ NULL                 │ ← new
└────┴───────┴────────┴──────────────────────┴──────────────────────┘
```

You just implemented SCD Type 2 in three commands.

#### Step 8. Reference the snapshot from a model:

```sql
-- models/marts/dim_customer_tiers_current.sql
{{ config(materialized='view') }}

select id, name, tier, dbt_valid_from
from {{ ref('customer_tiers_snap') }}
where dbt_valid_to is null
```

`ref()` works on snapshots exactly like it works on models. Snapshots are first-class DAG nodes.

### **2.7 Snapshot Production Tips**

- **Run snapshots on a separate schedule.** Don't bundle with model runs — snapshots write history that depends on time, so cadence matters. A common pattern: snapshots every hour, models every 4 hours.
- **Snapshots are stateful.** Don't drop them — you lose history. To rename, ALTER the warehouse table directly and update the snapshot file.
- **Use `timestamp` strategy whenever possible.** Cheaper than `check` (no hashing), more reliable.
- **`invalidate_hard_deletes=true` requires careful reasoning.** Once a row is "deleted" (expired), it'll come back as new on the next sync if the source restores it.
- **Snapshots don't honor `--full-refresh`.** Use `dbt snapshot --target prod` carefully; you can't rebuild history from current state.

### **2.8 Snapshot vs Incremental — Common Confusion**

| Aspect | Snapshot | Incremental |
|---|---|---|
| Purpose | Track history (SCD2) | Avoid full rebuild |
| Output schema | Source columns + dbt_valid_from/to/scd_id/updated_at | Source columns (unchanged) |
| Source assumptions | In-place mutations | Append-mostly or merge-able |
| Idempotent? | Yes (dedup on dbt_scd_id) | Yes if `unique_key` is set |
| Use case | Customer dimension with changing attributes | Event fact table |

---

## **3. SEEDS**

### **3.1 Mental Model**

A seed is a CSV file in your repo that becomes a table in the warehouse. Use it for small, slow-moving reference data: country code → name, status code → display label, FX rates as of a quarter, payment processor IDs, holiday calendars.

```
seeds/country_codes.csv  →  dbt seed  →  main_seeds.country_codes (table in warehouse)
```

### **3.2 Why It Exists**

Three options for reference data, before seeds:

1. **Hardcode `case when` mappings** in every model. DRY violation.
2. **Maintain a separate spreadsheet, manually load to warehouse.** Falls out of sync with code.
3. **Build a tiny ingestion pipeline** for one CSV. Overkill.

Seeds put the CSV in Git, version it with the code that uses it, and load it on demand. Perfect for small, code-adjacent reference data.

### **3.3 When to Use a Seed**

- ✅ Small (< 1000 rows ideally; under 10k is still fine)
- ✅ Slow-moving (changes once a quarter or less)
- ✅ Worth versioning with the code (review changes in PR)
- ❌ Large datasets — use proper ingestion (Fivetran, Airbyte)
- ❌ Frequently changing — too much PR churn
- ❌ Sensitive data (PII) — code repos are not data repos

### **3.4 Syntax**

#### A CSV under `seeds/`

```
# seeds/country_codes.csv
country_code,country_name,region
US,United States,North America
CA,Canada,North America
UK,United Kingdom,Europe
DE,Germany,Europe
JP,Japan,Asia
```

#### Run

```bash
dbt seed
```

dbt infers types from the CSV and creates `main_seeds.country_codes` in the warehouse.

#### Reference from a model

```sql
select
    o.order_id,
    c.country_name,
    c.region
from {{ ref('stg_orders') }} o
left join {{ ref('country_codes') }} c on o.country_code = c.country_code
```

`ref('country_codes')` — same function as for models. Seeds are DAG nodes.

### **3.5 Seed Configuration**

Configure seeds via `dbt_project.yml`:

```yaml
seeds:
  shop_dbt:
    +schema: seeds              # all seeds land in <target>_seeds schema
    country_codes:
      +column_types:
        country_code: varchar(2)
        country_name: varchar(100)
        region: varchar(50)
    fx_rates:
      +column_types:
        rate: numeric(18, 6)
```

The `+column_types` override is critical. dbt's CSV type inference is conservative — explicit types prevent string truncation and rounding bugs.

### **3.6 Tests on Seeds**

Seeds are DAG nodes, so they get tests like models:

```yaml
seeds:
  - name: country_codes
    columns:
      - name: country_code
        data_tests: [unique, not_null]
      - name: country_name
        data_tests: [not_null]
```

### **3.7 Build Along — Seed**

#### Step 1. Create `seeds/order_status_labels.csv`:

```
status_code,status_label,is_terminal
shipped,Shipped to customer,true
pending,Awaiting fulfillment,false
cancelled,Cancelled by customer,true
returned,Returned and refunded,true
```

#### Step 2. Configure `dbt_project.yml`:

```yaml
seeds:
  shop_dbt:
    order_status_labels:
      +column_types:
        status_code: varchar(20)
        status_label: varchar(100)
        is_terminal: boolean
```

#### Step 3. Load:

```bash
dbt seed
```

Expected:

```
1 of 1 START seed file main.order_status_labels  [RUN]
1 of 1 OK loaded seed file main.order_status_labels  [INSERT 4 in ...s]
```

#### Step 4. Use it:

```sql
-- models/marts/orders_with_status.sql
select
    o.order_id,
    o.status,
    s.status_label,
    s.is_terminal
from {{ ref('stg_orders') }} o
left join {{ ref('order_status_labels') }} s on o.status = s.status_code
```

#### Step 5. Run and verify:

```bash
dbt run --select orders_with_status
duckdb /tmp/dbt_workspace/shop.duckdb \
  "SELECT * FROM main.orders_with_status"
```

### **3.8 Updating Seeds**

Edit the CSV. Then:

```bash
dbt seed --full-refresh        # truncate and reload
```

Without `--full-refresh`, dbt's default behavior is `truncate + insert` for seeds (depends on version), so usually editing + `dbt seed` is enough. Use `--full-refresh` if you change column types — without it, dbt errors on type mismatch.

---

## **4. SNAPSHOTS vs SEEDS vs SOURCES — DECISION MATRIX**

| You have… | Use |
|---|---|
| A raw table loaded by Fivetran/Airbyte | `source()` |
| A small, slow-moving CSV checked into Git | Seed |
| A source table that mutates in place and you need history | Snapshot of the source |
| A small lookup table that the dbt project owns | Seed |
| A "raw" snapshot you wrote yourself outside dbt | Source (point dbt at it) |

---

## **5. REAL-WORLD USE CASES**

**Snapshots:**
- Customer subscription tier (Bronze/Silver/Gold) changes over time → snapshot the customers table.
- Product catalog with price changes → snapshot products. Time-travel queries become "what was the price on the day this order was placed?"
- Employee org chart changes → snapshot employees. Manager-of-manager queries on historical date.
- Salesforce opportunity stage progressions → snapshot opportunities; build a stage-history fact from snapshot history.

**Seeds:**
- ISO country/currency codes.
- Holiday calendars (rebuild yearly via PR).
- Mappings: error code → human-readable name, internal tenant code → marketing brand name.
- Static dimension tables for small finite enums (regions, product categories).
- "Lookup" tables that change quarterly, owned by the analytics team, code-adjacent.

---

## **6. BEST PRACTICES & ANTI-PATTERNS**

### **Snapshots — best practices**

- **Always use `target_schema='snapshots'`** — keep them separate from models, so a `dbt seed/run` accidental rebuild can't destroy history.
- **Use `timestamp` strategy unless you can't.** Cheaper, more reliable.
- **Test snapshots like models.** `unique` on `dbt_scd_id`, `not_null` on `dbt_valid_from`.
- **Document the SCD2 columns** so analysts know how to query history.
- **Run snapshots BEFORE downstream models** that need the latest version.

### **Snapshots — anti-patterns**

- **Snapshots in the same schema as models.** A misfire of `--full-refresh` could destroy history.
- **Modifying the snapshot SELECT later.** If you need different columns, create a new snapshot and a transition plan.
- **Using `check` strategy on a wide table.** Hashing 50 columns every run wastes compute.
- **Treating snapshot as a model.** It's a separate node type; you run it via `dbt snapshot`, not `dbt run`.

### **Seeds — best practices**

- **Always set `column_types`.** Explicit beats inferred.
- **Test seeds.** `unique` on the key column.
- **Keep seeds small.** > 10k rows → use proper ingestion.
- **Use seeds for code-adjacent reference data only.** PII never goes here.

### **Seeds — anti-patterns**

- **Multi-megabyte CSVs in Git.** PRs get unreadable, repos bloat.
- **Frequently changing seeds.** A CSV that changes weekly is real data; ingest it properly.
- **Sensitive data in seeds.** Repos are not data warehouses.
- **Skipping `--full-refresh` after type changes** → silent type coercion bugs.

---

## **7. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What's a dbt snapshot?**

**Model answer:** A snapshot is a dbt-maintained table that captures point-in-time history of a source table — SCD Type 2. Each row in the snapshot has `dbt_valid_from` / `dbt_valid_to` columns. When the source row changes, dbt expires the existing snapshot row (sets `dbt_valid_to = now()`) and inserts a new version. You query the snapshot for current state with `where dbt_valid_to is null`, or for historical state with a date predicate against the validity window.

### **Q2. [Foundational] What's a seed?**

**Model answer:** A seed is a CSV file under `seeds/` that dbt loads into the warehouse as a table on `dbt seed`. Used for small, slow-moving reference data — country codes, status labels, holiday calendars — that belongs in the same Git repo as the dbt code. Seeds are DAG nodes: you reference them with `ref('seed_name')`. They're tested and documented like models.

### **Q3. [Foundational] What's the difference between the `timestamp` and `check` snapshot strategies?**

**Model answer:** `timestamp` requires the source to have a column (often `updated_at`) that increments whenever the row changes. dbt compares the source row's timestamp to the snapshot's last-known timestamp; if newer, expire and insert. `check` strategy is for sources without a reliable updated-at column — dbt computes a hash of the listed columns each run and compares to the prior hash. `timestamp` is preferred (cheaper, more reliable). Use `check` when the source schema doesn't expose change metadata.

### **Q4. [Intermediate] How do you query a snapshot for "what was customer 42's tier on August 15, 2025?"**

**Model answer:**

```sql
select tier
from snapshots.customer_tiers_snap
where id = 42
  and dbt_valid_from <= '2025-08-15'
  and (dbt_valid_to > '2025-08-15' or dbt_valid_to is null)
```

The validity window is `[dbt_valid_from, dbt_valid_to)` — half-open — so the predicate excludes the upper bound. `dbt_valid_to is null` means "still current," so we OR that case in.

### **Q5. [Intermediate] Why are snapshots in a separate folder and run with a separate command?**

**Model answer:** Snapshots have different lifecycle semantics. Models can be dropped and rebuilt freely; snapshots are stateful — dropping one loses history. Snapshots also typically run on a different schedule (more frequently than the model build) because they need to capture changes at high cadence. The separate `snapshot-paths`, separate `dbt snapshot` command, and convention to use a separate `target_schema` enforce that separation. They still appear in the DAG and are referenceable via `ref()`.

### **Q6. [Advanced] When should you NOT use a dbt seed?**

**Model answer:** When the data is large (>10k rows), changes frequently (weekly+), is sensitive (PII, credentials), or really should be ingested. A seed in Git makes every change a code change — fine for a quarterly FX rate, painful for daily updates. Big seeds also bloat the repo and slow down `dbt seed`. The honest test: would a stakeholder be uncomfortable seeing this data in the public repo, or would they expect it to be ingested with other operational data? If yes to either, don't use a seed.

### **Q7. [Advanced] What happens if you change the columns in a snapshot SELECT after it's been running for a year?**

**Model answer:** dbt errors on the next snapshot run because the schema doesn't match the existing snapshot table. There's no clean dbt-native rebuild — `--full-refresh` destroys history. The recovery options are: (1) leave the existing snapshot alone and create a new snapshot with the new columns, build a transition view that unions both; (2) run a one-time SQL migration that adds the new columns to the existing snapshot table with NULLs for historical rows, then update the SELECT and continue. Option 2 preserves history but loses the early values for new columns. Snapshot schema evolution is a known dbt rough edge.

---

## **8. GOTCHAS**

- **Snapshots have no `--full-refresh` semantics.** `dbt snapshot --full-refresh` does nothing useful — it can't rebuild history. To rebuild a snapshot, drop the table manually and rerun.
- **`unique_key` must be stable.** If a source's PK rotates over time, snapshots accumulate orphaned rows.
- **`updated_at` time zones.** dbt compares timestamps as-is. Mixing TZ-aware and naive timestamps causes silent mis-detection of changes.
- **`check_cols=['*']` ignores the `unique_key` column.** Don't put the PK in `check_cols` — it's used as the join key, not a comparison column.
- **Seeds are `truncate + insert` by default.** A failed `dbt seed` mid-load can leave the seed empty. Use `+full_refresh: true` and a transactional adapter for safety.
- **Seed type inference** can pick the wrong type (e.g., `1` interpreted as int when you wanted varchar). Always set `column_types`.
- **Seeds + production deployment.** In some shops, seeds are deployed via a separate process (`dbt seed --target prod`) and not on every model run. Coordinate scheduling.

---

## **NEXT STEP**

You can capture history and load reference data. Now learn how to expose your work — documentation and lineage.

Go to [`07_documentation_and_lineage.md`](07_documentation_and_lineage.md).
