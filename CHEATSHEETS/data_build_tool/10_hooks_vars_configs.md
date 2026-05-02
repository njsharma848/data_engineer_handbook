# **10 — Hooks, Vars, and Configs**

> **Goal:** Round out production knowledge with the smaller-but-essential tools: pre/post hooks, project vars, environment-aware `profiles.yml`, and config precedence.

---

## **1. THREE TOPICS, ONE FILE**

These are the tools that turn a dbt project from "runs locally" into "runs in production":

- **Hooks** — SQL that runs before/after models and runs (grants, vacuums, audit logging).
- **Vars** — runtime parameters injectable from CLI or `dbt_project.yml`.
- **Configs** — the precedence and inheritance rules for everything dbt is configurable about.

Plus a deeper look at `profiles.yml` patterns for multi-environment deploys.

---

## **2. HOOKS**

### **2.1 Mental Model**

A hook is a SQL statement dbt runs **around** something:

| Hook | When it runs |
|---|---|
| `pre_hook` | Before each model | per-model |
| `post_hook` | After each model | per-model |
| `on-run-start` | Once at the start of a `dbt run/build/snapshot/seed` invocation | per-run |
| `on-run-end` | Once at the end | per-run |

Hooks let you do warehouse-side bookkeeping: granting permissions, logging runs, cleaning up old partitions, refreshing materialized views — anything that's SQL but lives outside the model's SELECT.

### **2.2 Why hooks exist**

Without hooks you'd schedule grants in cron, log runs from Python, vacuum tables in a separate job. Hooks let those concerns live next to the model, run by dbt, on every invocation.

### **2.3 Pre and post hooks — per model**

```sql
-- models/marts/core/dim_customers.sql
{{ config(
    materialized='table',
    post_hook=[
      "GRANT SELECT ON {{ this }} TO ROLE analyst",
      "ANALYZE {{ this }}"
    ]
) }}

select ...
```

Multiple hooks run in order. `{{ this }}` resolves to the model's fully-qualified name.

### **2.4 Project-wide hooks**

In `dbt_project.yml`:

```yaml
models:
  shop_dbt:
    +post-hook:
      - "GRANT SELECT ON {{ this }} TO ROLE analyst"
    marts:
      +post-hook:
        - "GRANT SELECT ON {{ this }} TO ROLE bi_tools"
```

Every model under the project gets the analyst grant; marts additionally get the bi_tools grant. Hooks are **additive**, not overriding.

### **2.5 `on-run-start` and `on-run-end`**

Run once per dbt invocation. Useful for run-level audit:

```yaml
# dbt_project.yml
on-run-start:
  - "INSERT INTO audit.dbt_runs VALUES ('{{ invocation_id }}', '{{ run_started_at }}', 'started')"

on-run-end:
  - "INSERT INTO audit.dbt_runs VALUES ('{{ invocation_id }}', current_timestamp, 'finished')"
  - "{{ dbt_utils.collect_freshness_results() }}"
```

Variables available in hooks include `invocation_id` (a per-run UUID), `run_started_at`, `target.name`, `target.schema`.

### **2.6 Real-world hook patterns**

**Grants (Snowflake):**
```yaml
+post-hook:
  - "GRANT USAGE ON SCHEMA {{ this.schema }} TO ROLE analyst_role"
  - "GRANT SELECT ON {{ this }} TO ROLE analyst_role"
```

**BigQuery: setting partition expiration:**
```yaml
+post-hook: "ALTER TABLE {{ this }} SET OPTIONS (partition_expiration_days = 365)"
```

**Audit row counts:**
```yaml
+post-hook: |
  INSERT INTO audit.row_counts
  SELECT '{{ this }}', count(*), '{{ invocation_id }}', current_timestamp
  FROM {{ this }}
```

**Vacuum after a heavy load (Postgres/Redshift):**
```yaml
+post-hook: "VACUUM ANALYZE {{ this }}"
```

### **2.7 Hook gotchas**

- **Hooks run in the same transaction as the model.** A failed hook fails the model build. Test hooks before shipping.
- **`pre_hook` runs before the model — including before the table exists.** `select * from {{ this }}` in a pre_hook fails on the first build.
- **`on-run-end` runs even on failure** if you've configured it that way (`on_failure: 'continue'`). Default is "skip on prior failure."
- **Hook order matters** when multiple are defined. List configs are concatenated; project-level + model-level both apply.
- **Hooks can't `select` results back into Python.** They're fire-and-forget DDL/DML.

---

## **3. VARS**

### **3.1 Mental Model**

A var is a runtime parameter accessible via `{{ var('name') }}`. Set defaults in `dbt_project.yml`, override on the CLI.

```yaml
# dbt_project.yml
vars:
  start_date: '2024-01-01'
  payment_methods: ['credit_card', 'bank_transfer', 'paypal']
  process_full_history: false
```

Use in a model:
```sql
select * from {{ ref('stg_orders') }}
where order_date >= '{{ var("start_date") }}'
```

Override on CLI:
```bash
dbt run --vars '{"start_date": "2025-06-01"}'
```

Vars enable:

- **Per-environment behavior** — sample 1% in dev, 100% in prod.
- **Backfills** — `dbt run --vars '{"start_date": "2020-01-01", "process_full_history": true}'`.
- **Multi-tenant runs** — `--vars '{"tenant": "acme"}'` to filter every model to one tenant.
- **Configuration that shouldn't live in SQL** — list of countries, FX cutoff date, etc.

### **3.2 Vars vs env_vars**

| Use | Var | Env_var |
|---|---|---|
| Project-level config (start dates, lists, flags) | ✅ | ❌ |
| Secrets (passwords, API keys) | ❌ | ✅ |
| Environment routing (`profiles.yml`) | ❌ | ✅ |
| Per-run override from CLI | ✅ | ❌ (env vars set by shell) |

```yaml
# profiles.yml
shop_dbt:
  outputs:
    prod:
      type: snowflake
      password: "{{ env_var('SF_PASSWORD') }}"
```

`env_var('SF_PASSWORD')` reads from the OS environment. Use `env_var('NAME', 'default')` for a fallback. **Never put passwords in `dbt_project.yml` — that file is committed.**

### **3.3 Default values for vars**

```sql
where order_date >= '{{ var("start_date", "2020-01-01") }}'
```

Two-arg form. Returns the default if the var isn't defined.

### **3.4 Per-package vars**

When a package needs config, scope vars to it:

```yaml
vars:
  dbt_utils:
    surrogate_key_treat_nulls_as_empty_strings: true
```

Inside the package, `{{ var('surrogate_key_treat_nulls_as_empty_strings', false) }}` reads it.

### **3.5 Conditional behavior using vars**

```sql
{{ config(
    materialized='incremental' if var('process_full_history', false) else 'table'
) }}

select *
from {{ ref('raw_events') }}
{% if var('start_date') %}
  where event_date >= '{{ var("start_date") }}'
{% endif %}
```

The `{% if var(...) %}` pattern is how you turn a var into a conditional WHERE clause.

---

## **4. `target` AND `profiles.yml` PATTERNS**

### **4.1 The `target` object**

Every model can read `target.name`, `target.schema`, `target.database`, `target.type` at compile time:

```sql
select *
from {{ ref('big_fact') }}
{% if target.name == 'dev' %}
  limit 1000
{% endif %}
```

In dev: only 1000 rows. In prod: full table. Same code, two behaviors.

### **4.2 Multi-target `profiles.yml`**

```yaml
shop_dbt:
  target: dev                    # default if --target not passed
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SF_ACCOUNT') }}"
      user: "{{ env_var('SF_USER') }}"
      password: "{{ env_var('SF_PASSWORD') }}"
      role: dev_role
      warehouse: dev_wh
      database: dev_db
      schema: dbt_{{ env_var('USER') }}    # personal schema per dev
      threads: 4

    ci:
      type: snowflake
      account: "{{ env_var('SF_ACCOUNT') }}"
      user: "{{ env_var('SF_CI_USER') }}"
      password: "{{ env_var('SF_CI_PASSWORD') }}"
      role: ci_role
      warehouse: ci_wh
      database: ci_db
      schema: "ci_{{ env_var('GITHUB_RUN_ID', '0') }}"
      threads: 8

    prod:
      type: snowflake
      account: "{{ env_var('SF_ACCOUNT') }}"
      user: "{{ env_var('SF_PROD_USER') }}"
      password: "{{ env_var('SF_PROD_PASSWORD') }}"
      role: prod_role
      warehouse: prod_wh
      database: prod_db
      schema: analytics
      threads: 16
```

Run with:
```bash
dbt run --target dev      # default
dbt run --target ci       # in GitHub Actions
dbt run --target prod     # production
```

Note the per-developer schema in dev (`dbt_alice`, `dbt_bob`) — every developer has their own dev schema, no collisions.

### **4.3 Personal dev schemas via `generate_schema_name`**

By default dbt prefixes the configured schema (`<target_schema>_<model_schema>`). Override:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}                  -- prod: literal schema
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}  -- dev/ci: prefix
    {%- endif -%}
{%- endmacro %}
```

In prod, `+schema: marts` writes to `marts`. In dev, it writes to `dbt_alice_marts`. Standard pattern in production projects.

### **4.4 Threads**

The `threads:` setting controls parallelism. dbt runs up to N nodes at once, bounded by:

- The DAG (parallel only when no edges).
- Warehouse concurrency limits (Snowflake default: 8 per warehouse).
- Network/connection pool.

Common values: dev=4, ci=8, prod=16+. Higher isn't always better — at some point connection setup overhead and warehouse queueing dominate.

---

## **5. CONFIG PRECEDENCE — THE MOST IMPORTANT RULES**

Configs cascade with this precedence (highest wins):

1. **Model-file `{{ config(...) }}`** — the most specific, always wins.
2. **YAML file `config:` block** for the model.
3. **`dbt_project.yml`** under `models:` (more specific path wins).
4. **dbt defaults**.

Example:

```yaml
# dbt_project.yml
models:
  shop_dbt:
    +materialized: view             # default for all models
    marts:
      +materialized: table          # marts override
      core:
        +materialized: table        # explicit, redundant with parent
```

```yaml
# models/marts/core/_core__models.yml
models:
  - name: dim_customers
    config:
      materialized: incremental     # YAML override
```

```sql
-- models/marts/core/dim_customers.sql
{{ config(materialized='table') }}    -- file wins; this is materialized as 'table'
```

**Resolution for `dim_customers`:** `table` (file beats YAML beats project).

For configs that take **lists** (hooks, tags), they're typically **additive** — the cascading layers all apply, not just the most specific. So a project-level `+post-hook: A` plus a model-level `post_hook=B` both run.

---

## **6. BUILD ALONG**

### **Step 1.** Add a project-wide audit hook in `dbt_project.yml`:

```yaml
on-run-end:
  - |
    create table if not exists audit_dbt_runs (
      invocation_id varchar,
      target_name varchar,
      run_started_at timestamp,
      run_completed_at timestamp
    );
  - |
    insert into audit_dbt_runs
    values (
      '{{ invocation_id }}',
      '{{ target.name }}',
      '{{ run_started_at }}',
      current_timestamp
    );
```

### **Step 2.** Add per-model post-hook on `fct_customer_orders`:

```sql
{{ config(
    materialized='table',
    post_hook="insert into audit_row_counts select '{{ this }}', count(*), current_timestamp from {{ this }}"
) }}
...
```

### **Step 3.** Pre-create the audit table:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb \
  "create table if not exists main.audit_row_counts (model varchar, row_count bigint, captured_at timestamp)"
```

### **Step 4.** Add a var-driven sample limit. Edit `models/marts/core/fct_customer_orders.sql`:

```sql
{{ config(materialized='table') }}

select
    c.customer_id,
    c.name,
    coalesce(s.orders_count, 0) as orders_count,
    coalesce(s.lifetime_value, 0) as lifetime_value,
    s.first_order_date,
    s.last_order_date
from {{ ref('stg_shop__customers') }} c
left join {{ ref('int_customer_orders_summary') }} s using (customer_id)
{% if target.name == 'dev' and var('sample_limit', 0) > 0 %}
limit {{ var('sample_limit') }}
{% endif %}
```

### **Step 5.** Run with the sample limit:

```bash
dbt run --select fct_customer_orders --vars '{"sample_limit": 2}'
```

The compiled SQL has `limit 2`. In prod (without the var) it has no limit.

### **Step 6.** Verify:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb \
  "select count(*) from main.fct_customer_orders"     # 2
duckdb /tmp/dbt_workspace/shop.duckdb \
  "select * from main.audit_row_counts order by captured_at desc limit 1"
```

The audit table records the row count for the run. The post-hook fired.

---

## **7. ENVIRONMENT-AWARE PATTERNS**

A handful of patterns appear in nearly every production dbt repo:

### **7.1 Different schemas per env**

```yaml
# profiles.yml
prod:
  schema: analytics
dev:
  schema: dbt_{{ env_var('USER') }}
```

### **7.2 Disable expensive models in dev**

```yaml
models:
  shop_dbt:
    huge_aggregations:
      +enabled: "{{ target.name == 'prod' }}"
```

`enabled: false` makes dbt skip the model entirely — no compile, no run.

### **7.3 Different sample sizes**

```sql
{% if target.name == 'dev' %}
  where created_at >= current_date - interval '7 days'
{% endif %}
```

### **7.4 Different cluster keys / partitioning**

```yaml
models:
  shop_dbt:
    huge_table:
      +cluster_by: "{{ ['date_col'] if target.name == 'prod' else [] }}"
```

### **7.5 Don't grant in dev**

```yaml
models:
  shop_dbt:
    +post-hook:
      - "{{ 'GRANT SELECT ON ' ~ this ~ ' TO ROLE analyst' if target.name == 'prod' else '' }}"
```

The empty string in dev is a no-op.

---

## **8. REAL-WORLD USE CASES**

- **Backfill via CLI vars.** `dbt run --select fct_orders --vars '{"start_date": "2020-01-01"}' --full-refresh`.
- **Per-tenant runs.** `dbt run --vars '{"tenant_id": "acme"}'` re-runs the project filtered to one tenant. Alternative to fully separate dbt projects.
- **Audit logging via on-run hooks.** Every dbt run inserts a row in `audit.dbt_runs` with status. Used for SLA reporting and monitoring.
- **Snowflake grant management.** `+post-hook` grants on every model, idempotent on every run. No drift.
- **Cluster key management.** Models that need clustering set `+cluster_by` in YAML; dbt issues `ALTER TABLE … CLUSTER BY` when needed.
- **CI cleanup.** `on-run-end` drops the CI schema after the run completes.

---

## **9. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Hooks for warehouse-side concerns only** — grants, vacuums, ANALYZE, audit. Never application logic.
- **Vars for runtime parameters** — start dates, sample limits, tenant filters.
- **`env_var()` for everything secret.** Passwords, API keys, OAuth tokens.
- **Per-developer dev schemas** via `generate_schema_name`. Prevents collisions.
- **`enabled: false` to disable expensive models in dev** rather than guard with `if target.name == 'prod'`.
- **List `vars` defaults in `dbt_project.yml`** for discoverability.
- **`on-run-end` for audit logging.** Cheap, high-value.

### **Anti-patterns**

- **Business logic in hooks.** "Insert this row at end of run" → that's a model, not a hook.
- **Hardcoded passwords** in `profiles.yml`. Always `env_var()`.
- **Vars used as feature flags** that never get cleaned up. Add a sunset date.
- **`pre_hook` that selects from `{{ this }}`** on the first run — fails because the table doesn't exist. Use `on-run-start` for project-level setup.
- **Hooks that depend on state from another hook** in unpredictable order. Make hooks independent.
- **Using `target.name` to gate prod-only dangerous ops.** Use `enabled` or a real env var.

---

## **10. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What's the difference between `pre_hook`, `post_hook`, and `on-run-start` / `on-run-end`?**

**Model answer:** `pre_hook` and `post_hook` run **per model** — before and after each model's main statement. `on-run-start` and `on-run-end` run **once per dbt invocation** — at the very beginning and end of `dbt run/build/snapshot/seed`. So if you run 50 models, pre/post hooks fire 100 times (50×2); on-run-start/end fire once each. Use pre/post for warehouse bookkeeping per model (grants, vacuums); use on-run for invocation-level concerns (audit logging, schema setup).

### **Q2. [Foundational] What's the difference between `var()` and `env_var()`?**

**Model answer:** `var()` reads project variables defined in `dbt_project.yml` under `vars:` or overridden on the CLI with `--vars`. `env_var()` reads OS environment variables. Use `var()` for project-level config (start dates, lists, flags) — it's discoverable in the repo. Use `env_var()` for secrets and CI-injected values (passwords, API keys, build IDs) — they shouldn't live in source control.

### **Q3. [Intermediate] How do you make the same dbt project behave differently in dev and prod?**

**Model answer:** Two main mechanisms:
1. **`profiles.yml` targets** — different schema, database, threads per target. `dbt run --target prod` switches the target.
2. **Conditional logic in models/configs** — `{% if target.name == 'prod' %}…{% endif %}`. Combine with vars and `enabled` to disable models in dev, sample data in dev, skip grants in dev, etc.

The pattern in real projects: per-developer dev schema (`dbt_alice`), shared CI schema (`ci_<run_id>`), single prod schema (`analytics`). Conditional logic for sampling, grants, partitioning.

### **Q4. [Intermediate] Walk me through config precedence in dbt.**

**Model answer:** Most specific wins. Order from highest to lowest priority:
1. `{{ config(...) }}` in the model file.
2. `config:` block in the model's YAML.
3. `dbt_project.yml` under `models:`, with deeper paths winning (`models/marts/core` beats `models/marts` beats `models/`).
4. dbt defaults.

For configs that take lists (hooks, tags, meta), all levels are **additive** — they all apply, not just the most specific. So a project-wide `post-hook` plus a model-specific `post_hook` both fire.

### **Q5. [Advanced] How would you implement per-developer dev schemas?**

**Model answer:** Override the `generate_schema_name` macro. Stock dbt prefixes the configured schema (`<target>_<model_schema>`). Replace it with logic that:
- In prod: returns the literal schema name (e.g., `analytics`).
- In dev/CI: prefixes with the target schema (e.g., `dbt_alice_marts`).

Then in `profiles.yml` set `schema: dbt_{{ env_var('USER') }}` for the dev target. Every developer gets their own schema, models still write to logical sub-schemas (`marts`, `staging`), no collisions, prod stays clean.

### **Q6. [Advanced] You need to backfill a year of data through your incremental model. How do you do it cleanly?**

**Model answer:**

```bash
dbt run --select fct_orders+ --vars '{"start_date": "2025-01-01"}' --full-refresh
```

The `--full-refresh` rebuilds the incremental table with the wider window from `start_date`. The `+` selector includes downstream models so they pick up the backfilled data. The `var` lets the SELECT widen its window; without it the model uses the default high-water-mark.

For very large backfills, `microbatch` strategy with `--event-time-start` / `--event-time-end` flags processes the backfill in time-bounded chunks rather than one giant transaction:

```bash
dbt run --select fct_orders --event-time-start 2025-01-01 --event-time-end 2026-05-01
```

Better than one full-refresh because it's resumable and bounded per chunk.

---

## **11. GOTCHAS**

- **Hooks run inside the model's transaction.** A failing hook fails the model. Test hooks in dev first.
- **`{{ this }}` in `pre_hook`** is valid syntax but the table may not exist on first build → some hooks fail.
- **`var()` with no default and no value defined** errors with "var not defined." Always set a default unless the var is required.
- **`env_var()` with no default and unset** errors. Use `env_var('NAME', 'default')`.
- **Vars from CLI must be valid JSON.** `--vars '{"x": 1}'` works; `--vars 'x=1'` doesn't (older dbt accepted it; modern dbt requires JSON).
- **`target.name` is the target's name** (`dev`, `prod`), not its type (`snowflake`, `bigquery`). Use `target.type` for the latter.
- **`enabled: false`** removes the model from the DAG entirely. `ref()` to a disabled model errors.
- **Hooks added retroactively** don't apply to existing rows. A `+post-hook: GRANT` only grants on subsequent runs.
- **`generate_schema_name` is one of the few dbt-built-in macros you'll override.** It's expected; the dbt docs guide you.
- **`on-run-end` fires even if the run failed** by default. Use `on_failure: 'continue'` for hooks that should always run.

---

## **NEXT STEP**

You've got the production scaffolding. Now learn how to ship it safely — CI/CD and debugging.

Go to [`11_cicd_and_debugging.md`](11_cicd_and_debugging.md).
