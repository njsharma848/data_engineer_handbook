# **01 — Setup and Your First dbt Model**

> **Goal:** End this file with a running dbt project, a DuckDB warehouse on disk, and your first model executed. Every command below was actually run on a fresh machine while writing this file — the captured output is real, not invented.
>
> **Time:** ~30 minutes.

---

## **0. THE STACK WE'RE BUILDING**

```
~/work/shop_dbt/                  ← dbt project (Git repo)
   └─ models/  tests/  macros/    ← your SQL + tests + Jinja
        │
        │  dbt run/build
        ▼
~/work/shop.duckdb                ← a single-file warehouse (DuckDB)
                                     no server, no cloud, fully local
```

DuckDB is OLAP SQLite: a serverless analytics engine in a single file. It's perfect for learning dbt because there's nothing to install on AWS, no credentials to manage, and it speaks standard SQL.

---

## **1. INSTALL dbt-core + dbt-duckdb**

### **1.1 Create an isolated Python environment**

Don't pollute your system Python — dbt has a tight dependency graph.

```bash
python3 -m venv ~/.venvs/dbt_venv
source ~/.venvs/dbt_venv/bin/activate
```

After activation your prompt will show `(dbt_venv)` — every `pip` and `dbt` command from now on uses this isolated env.

### **1.2 Install**

```bash
pip install --upgrade pip
pip install dbt-core dbt-duckdb
```

`dbt-core` is the engine. `dbt-duckdb` is the **adapter** — the plugin that knows how to translate dbt's compiled SQL into DuckDB-flavored SQL and execute it against a DuckDB file. Every warehouse has its own adapter package: `dbt-snowflake`, `dbt-bigquery`, `dbt-databricks`, `dbt-postgres`, `dbt-redshift`, etc. The pattern is always the same: install Core + the adapter for your warehouse.

### **1.3 Verify install**

```bash
dbt --version
```

**Real output (captured at write-time):**

```
Core:
  - installed: 1.11.8
  - latest:    1.11.8 - Up to date!

Plugins:
  - duckdb: 1.10.1 - Up to date!
```

> **Note for the interview:** dbt Core releases minor versions every few months. As of 2026, the active line is 1.11.x. If a job posting says "dbt 1.x" they mean any version of Core. Snapshots and tests have evolved across versions — see the gotchas section.

---

## **2. SCAFFOLD A NEW PROJECT — `dbt init`**

```bash
mkdir -p ~/work && cd ~/work
dbt init shop_dbt --skip-profile-setup
```

`--skip-profile-setup` tells dbt not to walk you through the interactive profile wizard — we'll write `profiles.yml` by hand so you understand it.

**Real output:**

```
16:24:53  Running with dbt=1.11.8
16:24:53  Creating dbt configuration folder at /root/.dbt

Your new dbt project "shop_dbt" was created!
```

This created two things:

```
~/work/shop_dbt/        ← the project (committed to Git)
~/.dbt/                 ← per-user dbt configuration (NEVER committed)
```

### **2.1 What's in the project**

```bash
ls -la ~/work/shop_dbt/
```

```
.gitignore
README.md
analyses/         ← ad-hoc SQL that won't be run, just compiled
dbt_project.yml   ← the project's config file (THE CORE FILE)
macros/           ← Jinja macros (file 05)
models/           ← .sql files that become tables/views (file 02)
seeds/            ← static CSVs (file 06)
snapshots/        ← SCD Type 2 captures (file 06)
tests/            ← singular tests (file 04)
```

Each folder corresponds to a "node type" in the dbt DAG. Don't rename them — the paths are configurable in `dbt_project.yml`, but conventions matter.

### **2.2 `dbt_project.yml` — the project's brain**

```yaml
name: 'shop_dbt'
version: '1.0.0'
profile: 'shop_dbt'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  shop_dbt:
    example:
      +materialized: view
```

Line-by-line:

- `name: 'shop_dbt'` — the project's logical name. Used in `ref()` calls and in the YAML key under `models:`.
- `version: '1.0.0'` — your project's semver. Bump it when you make breaking changes.
- `profile: 'shop_dbt'` — points at a top-level entry in `~/.dbt/profiles.yml`. **This is the link between code (in Git) and credentials (not in Git).**
- `model-paths: ["models"]` — where dbt finds `.sql` model files. You can add multiple paths.
- `analysis-paths` / `test-paths` / `seed-paths` / `macro-paths` / `snapshot-paths` — same pattern for other node types.
- `clean-targets` — folders that `dbt clean` deletes. Always includes `target/` (compiled SQL) and `dbt_packages/` (installed packages).
- `models: shop_dbt: example: +materialized: view` — sets the default materialization for any model under `models/example/` to `view`. The `+` prefix marks "this is a config, not a sub-folder name."

---

## **3. CONFIGURE THE DUCKDB CONNECTION — `profiles.yml`**

Open (or create) `~/.dbt/profiles.yml`:

```yaml
shop_dbt:                  # must match `profile:` in dbt_project.yml
  target: dev              # default target when you run `dbt`
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt_workspace/shop.duckdb   # any local path
      threads: 4
    prod:                  # a second target for later
      type: duckdb
      path: /var/data/shop_prod.duckdb
      threads: 8
```

Line-by-line:

- `shop_dbt:` — top-level key, matches `profile: 'shop_dbt'` in `dbt_project.yml`.
- `target: dev` — the default target (output env) used when no `--target` flag is passed.
- `outputs:` — one entry per environment. `dev` and `prod` here.
- `type: duckdb` — tells dbt which adapter to use. Other warehouses: `snowflake`, `bigquery`, `databricks`, `postgres`, etc.
- `path:` — DuckDB-specific: the file location. For Snowflake you'd see `account`, `user`, `password`, `warehouse`, etc.
- `threads: 4` — how many models dbt may build in parallel. Bound by warehouse concurrency limits in real life.

**Critical:** `profiles.yml` lives in `~/.dbt/` (your home dir), **not** the Git repo, because it can contain secrets. The repo's `dbt_project.yml` only references the profile by name.

### **3.1 Verify connectivity — `dbt debug`**

```bash
cd ~/work/shop_dbt
dbt debug
```

**Real output:**

```
adapter version: 1.10.1
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]
Required dependencies:
 - git [OK found]

Connection:
  database: shop
  schema: main
  path: /tmp/dbt_workspace/shop.duckdb
  ...
Registered adapter: duckdb=1.10.1
  Connection test: [OK connection ok]

All checks passed!
```

If anything fails here — wrong path, missing adapter, profile name mismatch — `dbt debug` is the first place to look. It validates files, dependencies, and connection in that order.

---

## **4. RUN THE STARTER MODELS**

`dbt init` shipped two example models in `models/example/`. Run them as-is to confirm the install:

```bash
dbt run
```

**Real output:**

```
16:25:08  Running with dbt=1.11.8
16:25:09  Unable to do partial parsing because saved manifest not found. Starting full parse.
16:25:10  Found 2 models, 4 data tests, 475 macros

Concurrency: 4 threads (target='dev')

1 of 2 START sql table model main.my_first_dbt_model ........ [RUN]
1 of 2 OK created sql table model main.my_first_dbt_model ... [OK in 0.11s]
2 of 2 START sql view model main.my_second_dbt_model ........ [RUN]
2 of 2 OK created sql view model main.my_second_dbt_model ... [OK in 0.05s]

Finished running 1 table model, 1 view model in 0.27 seconds.

Completed successfully

Done. PASS=2 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=2
```

What just happened:

1. dbt parsed every `.sql` and `.yml` file under the project, building an internal **manifest** (a graph of all models, tests, sources, etc.).
2. It compiled each model's Jinja+SQL into pure DuckDB SQL — saved to `target/compiled/`.
3. It wrapped the compiled SQL in DDL (`CREATE TABLE AS …` for tables, `CREATE VIEW AS …` for views) — saved to `target/run/`.
4. It executed the wrapped DDL against DuckDB, in DAG order, with 4-way parallelism.
5. It wrote `target/manifest.json` and `target/run_results.json` — these are the artifacts that everything (docs, CI, lineage tools) reads.

You now have a DuckDB file with two real tables:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb "SELECT * FROM main.my_first_dbt_model"
```

```
┌───────┐
│  id   │
│ int32 │
├───────┤
│   1   │
│ NULL  │   ← this NULL is intentional — file 04 will fail a test on it
└───────┘
```

---

## **5. WRITE YOUR FIRST REAL MODEL**

Replace the example folder with our consistent e-commerce domain.

### **5.1 Clean out the starters**

```bash
rm -rf models/example/
```

Now `models/` is empty.

### **5.2 Drop in some seed raw data**

For now we'll fake "raw" data with a CTE inside the model. Files 03 and 06 will introduce sources and seeds properly.

Create `models/customers.sql`:

```sql
{{ config(materialized='table') }}

with raw_customers as (
    select 1 as customer_id, 'Alice' as name, 'alice@shop.com' as email
    union all select 2, 'Bob',   'bob@shop.com'
    union all select 3, 'Carol', 'carol@shop.com'
    union all select 4, 'Dan',   null
)

select
    customer_id,
    name,
    email,
    case when email is null then false else true end as has_email
from raw_customers
```

**Line-by-line:**

- `{{ config(materialized='table') }}` — Jinja directive. dbt evaluates this at compile time and uses it to decide HOW to physicalize the model. `'table'` means `CREATE TABLE AS …`. Other options in file 02.
- `with raw_customers as ( … )` — a normal SQL CTE. dbt does not care that this is fake data; in real life this would be `select * from {{ source('jaffle_shop', 'customers') }}`.
- `select … from raw_customers` — the model's actual output. Anything `select`ed at the bottom of the file becomes the table's schema.

### **5.3 Run just this model**

```bash
dbt run --select customers
```

**Real output (captured):**

```
Found 1 model, 0 data tests, 475 macros

1 of 1 START sql table model main.customers ........ [RUN]
1 of 1 OK created sql table model main.customers ... [OK in 0.06s]

Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
```

`--select customers` filters to that one model. dbt's selection grammar is rich (file 11): you can select by tag, by folder, by upstream/downstream, by changed-since-state.

### **5.4 Inspect the compiled SQL**

```bash
cat target/compiled/shop_dbt/models/customers.sql
```

You'll see your file with the `{{ config(...) }}` line stripped and Jinja resolved. The `target/run/` folder additionally wraps it in `CREATE TABLE main.customers AS …`.

> **Why this matters for interviews:** "dbt compiles to native warehouse SQL" is exactly what you can prove by reading these two folders. `target/compiled/` is the SELECT statement; `target/run/` is the SELECT plus the DDL wrapper.

---

## **6. BUILD ALONG — A 10-MINUTE EXERCISE**

By the end of this you'll have a 3-model project that mirrors what every dbt repo looks like.

### **Step 1.** Add `models/orders.sql`:

```sql
{{ config(materialized='view') }}

select 101 as order_id, 1 as customer_id, '2026-01-05' as order_date, 49.99 as amount
union all select 102, 1, '2026-01-08', 19.50
union all select 103, 2, '2026-01-09', 102.00
union all select 104, 3, '2026-01-12', 8.75
union all select 105, 1, '2026-01-15', 250.00
```

### **Step 2.** Add `models/customer_orders.sql` — your first model that joins two others:

```sql
{{ config(materialized='table') }}

select
    c.customer_id,
    c.name,
    count(o.order_id)        as orders_count,
    coalesce(sum(o.amount),0) as lifetime_value
from {{ ref('customers') }} as c
left join {{ ref('orders')  }} as o using (customer_id)
group by 1, 2
order by 3 desc
```

The two `{{ ref('…') }}` calls are dbt's lineage glue — file 03 explains them in depth.

### **Step 3.** Run the whole project:

```bash
dbt run
```

**Expected output:**

```
Found 3 models, 0 data tests, 475 macros

1 of 3 START sql table model main.customers .............. [OK]
2 of 3 START sql view  model main.orders ................. [OK]
3 of 3 START sql table model main.customer_orders ........ [OK]

Done. PASS=3 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=3
```

dbt ran them in dependency order: `customers` and `orders` in parallel (no deps), then `customer_orders` after both (because `ref('customers')` and `ref('orders')` made it depend on them).

### **Step 4.** Query the result:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb "SELECT * FROM main.customer_orders"
```

```
┌─────────────┬───────┬──────────────┬────────────────┐
│ customer_id │ name  │ orders_count │ lifetime_value │
├─────────────┼───────┼──────────────┼────────────────┤
│      1      │ Alice │      3       │     319.49     │
│      2      │ Bob   │      1       │     102.00     │
│      3      │ Carol │      1       │       8.75     │
│      4      │ Dan   │      0       │       0.00     │
└─────────────┴───────┴──────────────┴────────────────┘
```

You just built a 3-node DAG. **You have used dbt.**

---

## **7. THE 5-STEP "GET STARTED" CHECKLIST (matches the infographic)**

You've now done all five:

| Step | Status | Where |
|---|---|---|
| 1. Install dbt-core | ✅ | Section 1 |
| 2. Connect warehouse | ✅ | Section 3 |
| 3. Convert SQL → dbt model | ✅ | Section 5 |
| 4. Add a uniqueness test | ⏭ Next file (`04_tests.md`) | — |
| 5. `dbt build` | ⏭ File 04 | — |

---

## **8. REAL-WORLD USE CASES**

In production, the setup looks slightly different but the bones are identical:

- **Snowflake project:** swap `type: duckdb` for `type: snowflake`; add `account`, `user`, `password`, `warehouse`, `database`, `schema`. The model SQL doesn't change.
- **BigQuery project:** swap to `type: bigquery`; add `project`, `dataset`, `keyfile` (or use `oauth`). dbt translates `ref()` into BigQuery's `project.dataset.table`.
- **CI:** the `dbt debug` and `dbt build` commands are exactly what runs in GitHub Actions / GitLab CI.
- **Secret management:** never put passwords in `profiles.yml`. Use `{{ env_var('SNOWFLAKE_PASSWORD') }}`, store the secret in your CI provider, and inject it.

---

## **9. BEST PRACTICES**

- **Always commit `dbt_project.yml`. Never commit `profiles.yml`.** The `.gitignore` from `dbt init` already excludes `target/`, `dbt_packages/`, and `logs/`.
- **Pin your dbt version in CI** (`dbt-core==1.11.8`). dbt minor releases sometimes have small breaking changes.
- **Use `dbt build` over `dbt run`** as your default command. It runs models, tests, snapshots, and seeds in DAG order.
- **Run `dbt debug` first whenever something breaks** — it triages config / dependency / connection in that order.
- **Keep `target/` in `.gitignore`.** It's regenerated on every run and grows fast.

---

## **10. INTERVIEW QUESTIONS**

### **Q1. [Foundational] Walk me through what `dbt run` actually does.**

**Model answer:** `dbt run` parses every file in the project to build a manifest (the DAG of all models, tests, sources, etc.), compiles each model's Jinja+SQL into pure warehouse SQL stored in `target/compiled/`, wraps that compiled SQL in DDL like `CREATE TABLE AS …` or `CREATE VIEW AS …` stored in `target/run/`, then executes the DDL against the warehouse in DAG order with whatever parallelism `threads:` allows. Finally it writes `target/run_results.json` summarizing what passed, failed, or was skipped.

### **Q2. [Foundational] Why is `profiles.yml` separate from `dbt_project.yml`?**

**Model answer:** Separation of code and credentials. `dbt_project.yml` lives in the Git repo — it's the same for every developer. `profiles.yml` lives in `~/.dbt/` and contains secrets that vary per developer or environment. The link is the `profile:` key in `dbt_project.yml` which references a top-level entry in `profiles.yml`. This way the same code can target dev / staging / prod by switching `--target` without touching the codebase.

### **Q3. [Intermediate] What's the difference between `target/compiled/` and `target/run/`?**

**Model answer:** `target/compiled/` contains the SELECT statement after Jinja has been resolved — you can copy-paste it into a SQL client and run it. `target/run/` contains the same SELECT wrapped in the materialization-specific DDL (`CREATE TABLE … AS …`, `CREATE VIEW … AS …`, the merge statement for incrementals, etc.). Both are pure native-warehouse SQL — Jinja is gone.

### **Q4. [Intermediate] How do I parameterize the same project across dev and prod?**

**Model answer:** Multiple targets in `profiles.yml`. Each target has its own warehouse / database / schema. Run `dbt build --target dev` or `--target prod`. Inside SQL or YAML you can switch behavior by reading `{{ target.name }}` — for example, `if target.name == 'prod' then merge else append`. Secrets come from `{{ env_var('VAR_NAME') }}`, never hardcoded.

### **Q5. [Advanced] Why do dbt projects have separate `models/`, `seeds/`, `snapshots/`, `tests/`, `macros/`, `analyses/` folders? Couldn't they all live together?**

**Model answer:** Each folder maps to a different node type in the DAG with different lifecycle semantics. Models become tables/views and run on every `dbt run`. Seeds are static CSVs that load only on `dbt seed`. Snapshots are SCD Type 2 captures with their own state table; they only update on `dbt snapshot`. Tests are assertions, run on `dbt test`. Macros aren't run at all — they're imported into other files at compile time. Analyses are SQL that compiles but never executes — they just show up in compiled form for ad-hoc reuse. The folder boundary makes the DAG-engine's job tractable.

---

## **11. GOTCHAS**

- **Activated venv?** If `dbt --version` says "command not found," your venv isn't sourced. `source ~/.venvs/dbt_venv/bin/activate`.
- **`dbt init` prompts you for a profile.** Use `--skip-profile-setup` to bypass and write `profiles.yml` by hand.
- **Multiple `profiles.yml` locations.** dbt searches `--profiles-dir`, `$DBT_PROFILES_DIR`, then `~/.dbt/`. If `dbt debug` says "Could not find profile," check which one it's looking at.
- **DuckDB file locks.** A DuckDB file can be opened by only one writer at a time. If `dbt run` says "could not be opened," another process (a notebook, a `duckdb` shell) is holding it.
- **Parallel writes to DuckDB.** `threads: 4` works fine — DuckDB internally serializes writes per file. On real warehouses, threads map to concurrent queries.
- **Schema = "main".** DuckDB defaults to the `main` schema. Snowflake/BigQuery use whatever you set in `profiles.yml`.
- **Jinja errors usually surface at parse time, not run time.** If you see "Compilation Error" in red, the issue is your Jinja, not your SQL.

---

## **NEXT STEP**

You have a running project. Now learn what `view` / `table` / `incremental` / `ephemeral` actually buy you.

Go to [`02_models_and_materializations.md`](02_models_and_materializations.md).
