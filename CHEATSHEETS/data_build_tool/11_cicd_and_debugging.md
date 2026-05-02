# **11 — CI/CD and Debugging**

> **Goal:** Run dbt safely in production. Cover Slim CI with `state:modified+`, GitHub Actions templates, the artifacts (`manifest.json`, `run_results.json`), and the debugging workflow.

---

## **1. THE MENTAL MODEL**

dbt projects need the same engineering rituals as application code:
- **CI on every PR**: build only changed models, run their tests, fail the merge if any fail.
- **CD on merge**: deploy to prod (run, snapshot, refresh docs).
- **Observability**: every run leaves an artifact (`run_results.json`); persist it for postmortems.
- **Debugging**: when something breaks, you read compiled SQL — not the Jinja source.

---

## **2. WHY CI MATTERS**

Without CI:
- Bad SQL merged → broken production run at 3am → page.
- Test added that fails on prod data → no warning until prod run.
- Schema change breaks 30 downstream models → discovered by stakeholders.

With CI:
- Every PR builds the changed models in an ephemeral schema.
- All affected tests run.
- Merge is blocked if any fail.
- Stakeholders never see the bug.

The killer feature is **Slim CI** — only build models *changed* by the PR plus their downstream blast radius. A 1000-model repo doesn't fully rebuild on every PR; it builds the 5 changed models and the 12 downstream of them. Minutes instead of hours.

---

## **3. SLIM CI — `state:modified+`**

### **3.1 The mechanism**

dbt's `--defer` and `--state` flags compare the current code's manifest to a saved manifest from prod. The selector `state:modified+` resolves to "every model whose code differs from the saved manifest, plus everything downstream."

```bash
# Compare current branch to prod's saved manifest
dbt build --select state:modified+ --defer --state ./prod-manifest
```

What dbt does:
1. Loads `./prod-manifest/manifest.json` (the prod manifest, downloaded by CI before this step).
2. Computes which models differ in the current branch.
3. Selects those + downstream.
4. **Defer** — for any unchanged model that's a dependency, don't rebuild it; reference the prod copy via `--defer`.
5. Build only the diff.

### **3.2 Concrete CI flow**

```
prod nightly run
   │
   ├── dbt run + dbt test
   │
   └── upload target/manifest.json to S3
              ↓
   ──────────────────────────────────────────
              ↓
   PR opened
   │
   ├── checkout PR branch
   ├── download prod's manifest.json from S3
   ├── dbt deps
   ├── dbt build --select state:modified+ \
   │            --defer --state ./prod-manifest \
   │            --target ci
   └── tests pass → merge unblocked
                  → tests fail → merge blocked
```

### **3.3 GitHub Actions template**

```yaml
# .github/workflows/dbt-ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]

jobs:
  dbt-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - run: pip install dbt-core==1.11.8 dbt-snowflake==1.10.0

      - name: Download prod manifest
        run: aws s3 cp s3://my-dbt-artifacts/prod/manifest.json ./prod-manifest/manifest.json
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}

      - name: dbt deps
        run: dbt deps

      - name: dbt build (Slim CI)
        run: |
          dbt build \
            --select state:modified+ \
            --defer --state ./prod-manifest \
            --target ci
        env:
          DBT_PROFILES_DIR: ./
          SF_ACCOUNT: ${{ secrets.SF_ACCOUNT }}
          SF_USER: ${{ secrets.SF_CI_USER }}
          SF_PASSWORD: ${{ secrets.SF_CI_PASSWORD }}

      - name: Cleanup CI schema
        if: always()
        run: dbt run-operation drop_old_ci_schemas --target ci
```

The `Cleanup` step uses a custom macro to drop CI schemas after the build, preventing warehouse bloat:

```sql
-- macros/drop_old_ci_schemas.sql
{% macro drop_old_ci_schemas() %}
  {% if target.name != 'ci' %}
    {{ exceptions.raise_compiler_error("Refusing to drop schemas outside ci target") }}
  {% endif %}
  {% set schema_pattern = "ci_" ~ env_var('GITHUB_RUN_ID', '0') ~ "_%" %}
  {{ run_query("DROP SCHEMA IF EXISTS " ~ target.database ~ "." ~ schema_pattern ~ " CASCADE") }}
{% endmacro %}
```

### **3.4 Common state selectors**

| Selector | Meaning |
|---|---|
| `state:modified` | Models whose SQL or YAML changed |
| `state:modified+` | Modified + downstream |
| `state:modified+1` | Modified + one level downstream |
| `state:modified.body` | Only SQL body changed (not config) |
| `state:modified.configs` | Config changed (e.g., materialization) |
| `state:new` | Models that exist now but didn't in saved manifest |
| `result:error+` | Models that errored in last run + downstream |
| `result:fail+` | Models with failed tests in last run + downstream |

`result:error+` is invaluable in retry workflows: re-run only what broke.

---

## **4. THE ARTIFACTS — `manifest.json`, `run_results.json`, `catalog.json`**

dbt writes JSON artifacts to `target/` after every command. These power CI, observability, and tooling integrations.

### **4.1 `manifest.json`**

The full DAG. Every node, its config, its `compiled_code`, its dependencies. Used by:
- `state:modified+` (compares vs saved manifest)
- `dbt docs serve` (renders the site)
- Lineage tools (DataHub, Atlan, OpenLineage)

Inspect:
```bash
jq '.nodes | keys[]' target/manifest.json | head
jq '.nodes."model.shop_dbt.fct_customer_orders"' target/manifest.json
```

### **4.2 `run_results.json`**

Output of the last `dbt run/build/test/seed/snapshot`. Contains:
- Status of each node (success/error/skipped/pass/fail)
- Execution time
- Compiled code
- Error messages

```bash
jq '.results[] | select(.status != "success") | {unique_id, status, message}' target/run_results.json
```

This is your postmortem: what failed, why, how long it took.

### **4.3 `catalog.json`**

Output of `dbt docs generate`. Column types and table sizes from the warehouse's `information_schema`.

### **4.4 Persisting artifacts**

In production:
- After every prod run, upload `target/manifest.json` and `target/run_results.json` to S3/GCS.
- CI downloads `manifest.json` for state-based selection.
- Observability tools ingest `run_results.json` for run history.

### **4.5 Elementary, dbt-checkpoint, OpenLineage**

Production add-ons that consume artifacts:
- **Elementary** — observability dashboard built on artifact ingestion.
- **dbt-checkpoint** — pre-commit hooks that lint manifest contents (every model has a description, every PK has a unique test, etc.).
- **OpenLineage** — emit lineage events to a metadata store.

---

## **5. DEBUGGING WORKFLOW**

When a model fails, follow this checklist in order.

### **Step 1. Read the error message.**

dbt's errors are usually informative. The first place to look:
```
Database Error in model fct_orders (models/marts/core/fct_orders.sql)
  ambiguous column reference 'order_id'
  compiled code at target/run/shop_dbt/models/marts/core/fct_orders.sql
```

The path to the **compiled SQL** is right there. Read that next.

### **Step 2. Read the compiled SQL.**

```bash
cat target/compiled/shop_dbt/models/marts/core/fct_orders.sql      # the SELECT only
cat target/run/shop_dbt/models/marts/core/fct_orders.sql           # SELECT + DDL wrapper
```

The compiled SQL is what hit the warehouse — Jinja is gone. Easier to debug than the Jinja source.

### **Step 3. Run the compiled SQL in a SQL client.**

Copy-paste the compiled SELECT into your warehouse's SQL UI (Snowflake worksheet, BigQuery console, DuckDB shell). Reproduce the error. Iterate on it directly.

### **Step 4. `dbt compile --select <model>` for fast feedback.**

`dbt compile` renders Jinja without running anything. Use this when you're iterating on Jinja:

```bash
dbt compile --select fct_orders
cat target/compiled/shop_dbt/models/marts/core/fct_orders.sql
```

### **Step 5. `dbt show` for quick result inspection (1.6+).**

Run a SELECT against a model and print rows without materializing:

```bash
dbt show --select fct_orders --limit 10
```

Useful when debugging downstream of a model: does the data look right?

### **Step 6. `dbt parse` for parse errors.**

Catches Jinja syntax issues and unresolved `ref()`/`source()` without running:

```bash
dbt parse
```

### **Step 7. Check `run_results.json` for the full failure context.**

```bash
jq '.results[] | select(.unique_id == "model.shop_dbt.fct_orders")' target/run_results.json
```

You see the message, timing, status, and compiled code in JSON.

### **Step 8. `--log-level debug` for verbose tracing.**

```bash
dbt run --select fct_orders --log-level debug 2> debug.log
```

Shows every SQL statement dbt issues. For really gnarly bugs, this is the lowest level.

---

## **6. COMMON FAILURES AND FIXES**

| Symptom | Likely cause | Fix |
|---|---|---|
| `Compilation Error: Could not find ref('x')` | Typo in `ref()`, model file missing, model disabled | Check filename matches; `enabled: true`; same project |
| `Database Error: ambiguous column` | Two joined tables have the same column without alias | Alias the SELECT columns explicitly |
| `Test failed: 5 rows returned` | Real data quality issue or test too strict | Inspect failures via `store_failures: true` |
| `Materialization 'incremental' has no merge strategy` | Adapter doesn't support MERGE | Switch to `delete+insert` or `append` |
| `Schema mismatch in incremental` | New column, default `on_schema_change` | Set `on_schema_change: append_new_columns` |
| `Snapshot strategy 'check' check_cols not provided` | Missing `check_cols` in config | Add it or switch to `timestamp` strategy |
| `Could not find profile named 'shop_dbt'` | `profile:` in `dbt_project.yml` doesn't match `profiles.yml` key | Align names |
| `Invalid value for vars` | Bad JSON in `--vars` flag | Use proper JSON: `--vars '{"x": "y"}'` |
| Test passed locally, fails in CI | Different data; or `state:modified+` missed a dep | Check the failing rows in CI, then locally with same data |

---

## **7. WHAT TO RUN IN CI**

A complete CI step list, in order:

```bash
dbt deps                                          # install packages
dbt parse                                         # fast syntax check
dbt build --select state:modified+ \              # build + test changed models
          --defer --state ./prod-manifest \
          --target ci
dbt source freshness --target ci                  # source freshness check (optional)
dbt run-operation drop_old_ci_schemas             # cleanup
```

For full nightly prod:

```bash
dbt deps
dbt seed
dbt snapshot
dbt run --target prod
dbt test --target prod
dbt source freshness --target prod
dbt docs generate --target prod
aws s3 sync target/ s3://my-dbt-artifacts/prod/   # persist artifacts
```

---

## **8. BUILD ALONG — A LOCAL CI SIMULATION**

We don't have GitHub Actions in this terminal, but you can simulate Slim CI locally.

### **Step 1.** Create a "prod" snapshot of the current manifest:

```bash
cd ~/work/shop_dbt
dbt parse --target dev
mkdir -p ./prod-state
cp target/manifest.json ./prod-state/manifest.json
```

### **Step 2.** Make a change to one model. Edit `models/staging/shop/stg_shop__customers.sql`:

```sql
{{ config(materialized='view') }}

select
    id           as customer_id,
    full_name    as name,
    email,
    upper(email) as email_upper,        -- new column
    signup_date::date as signup_date
from {{ source('shop_raw', 'customers') }}
```

### **Step 3.** Run with state-modified selector:

```bash
dbt build --select state:modified+ --state ./prod-state
```

Expected: dbt rebuilds `stg_shop__customers` plus everything downstream — `int_customer_orders_summary` (if it referenced this model — in our project it doesn't, so it's not selected) and `fct_customer_orders` (which does ref this model).

```
Found 3 models, 8 tests, ...

1 of N START sql view  model main.stg_shop__customers ........ [OK]
2 of N START sql table model main.fct_customer_orders ........ [OK]
... tests ...
```

The unchanged staging models (e.g. `stg_shop__orders`) are NOT rebuilt. That's Slim CI in action.

### **Step 4.** Inspect what was selected:

```bash
dbt list --select state:modified+ --state ./prod-state
```

Output: just the modified model and its downstream nodes.

### **Step 5.** Inspect `run_results.json`:

```bash
jq '.results | length' target/run_results.json
jq '.results[] | {unique_id, status, execution_time}' target/run_results.json
```

Every node's status is right there — perfect for CI dashboards.

---

## **9. REAL-WORLD USE CASES**

- **PR-level CI** — Slim CI on every PR. 3-minute builds vs 30-minute full rebuilds.
- **Nightly prod runs via Airflow** — `BashOperator` with `dbt build`. Failures alert via Slack/PagerDuty.
- **Auto-revert on test failure** — CI/CD pipeline rolls back to previous tag if the prod run fails tests beyond a tolerance.
- **`result:error+` for retries** — manually re-run only the models that errored, not the whole DAG.
- **Cross-project deployment** — dbt Mesh: child projects pull parent's `manifest.json` to validate cross-project refs.
- **PR comments with affected models** — a CI step that posts "this PR rebuilds X models, runs Y tests, affects Z dashboards" via the manifest+exposures.

---

## **10. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Always `dbt build` in production**, not `dbt run` then `dbt test`. Build runs them in DAG order so failures stop downstream.
- **Persist `manifest.json` and `run_results.json` after every prod run.** Required for Slim CI; valuable for postmortems.
- **Pin dbt and adapter versions in CI.** `dbt-core==1.11.8`. Floating versions cause silent breakages.
- **Run `dbt deps` first in CI.** Packages must be present.
- **Use `--defer` with Slim CI** — unchanged models reference prod, saving CI compute.
- **Drop CI schemas after the run.** `run-operation` macro for cleanup.
- **Tag models for selective scheduling.** `dbt run --select tag:hourly`.

### **Anti-patterns**

- **Full rebuild on every PR.** Wastes warehouse $$, makes PRs slow, discourages frequent commits.
- **No CI gate at all.** PRs merge without ever running. Bugs hit prod.
- **Skipping `dbt deps` in CI.** Errors with "package not found"; confused engineers.
- **Reading Jinja source to debug a SQL error.** Read the compiled SQL.
- **Hardcoded credentials in CI config.** Always env vars from CI secrets.
- **`dbt run` with no `--select` in CI.** Builds everything, every time. Slim CI exists for a reason.
- **Forgetting to persist `manifest.json`.** Slim CI doesn't work without it.

---

## **11. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What's the difference between `dbt run` and `dbt build`?**

**Model answer:** `dbt run` runs models only. `dbt build` runs models, tests, snapshots, and seeds in DAG order — and importantly, on a test failure for an upstream model, it skips downstream models and tests. This is the production-correct command because it stops bad data from propagating. `dbt run` followed by `dbt test` runs all models first regardless of test outcomes; `build` interleaves and short-circuits on failure.

### **Q2. [Foundational] What is Slim CI?**

**Model answer:** Slim CI is dbt's pattern for CI that builds only what changed in a PR, instead of the whole project. It uses `state:modified+` to select models whose code differs from a saved prod manifest, plus their downstream. Combined with `--defer --state`, unchanged models reference prod's tables instead of being rebuilt. Result: PR builds in minutes instead of hours, warehouse cost stays low, fast feedback loops. Required artifact: `target/manifest.json` from a prior prod run, persisted somewhere CI can fetch.

### **Q3. [Foundational] What's `manifest.json` and why does it matter?**

**Model answer:** `manifest.json` is dbt's DAG dump — every model, source, test, snapshot, seed, exposure, with their configs, compiled SQL, and dependencies. Written to `target/` after every dbt invocation. Used by `dbt docs serve`, by `state:modified+` (comparing two manifests), and by external tools (DataHub, Elementary, OpenLineage). In production, you persist it after every run so CI can use it for state-based selection.

### **Q4. [Intermediate] How would you investigate a model that failed in prod last night?**

**Model answer:** First, get `target/run_results.json` from the failed run (assuming it's persisted to S3). Find the failing model:

```bash
jq '.results[] | select(.status != "success")' run_results.json
```

The output has the unique_id, error message, and compiled SQL path. Pull the compiled SQL from `target/compiled/`, paste it into a SQL client, and reproduce the error against prod data. If that's not enough, run with `--log-level debug` and tail the logs. The whole loop is read run_results → read compiled SQL → reproduce in SQL client → fix.

### **Q5. [Intermediate] How does `--defer` work and when do you use it?**

**Model answer:** `--defer --state <prod-manifest-dir>` tells dbt: when a model in the current run depends on a model that *isn't* being rebuilt this run, look up the deferred-to relation from the saved manifest and use that instead. So in CI, if you're rebuilding `fct_orders` but `dim_customers` is unchanged, the rebuild of `fct_orders` references prod's `dim_customers` table — no need to clone all of prod into the CI schema. Without `--defer`, the missing dependency would error or silently produce empty data.

### **Q6. [Advanced] Walk me through implementing Slim CI from scratch in GitHub Actions.**

**Model answer:**

1. **Persist `manifest.json` from prod**: in the prod nightly job, after `dbt build --target prod`, `aws s3 cp target/manifest.json s3://artifacts/prod/manifest.json`.
2. **PR workflow** (`.github/workflows/dbt-ci.yml`):
   - Checkout the PR branch.
   - Install dbt + adapter (pinned versions).
   - `aws s3 cp s3://artifacts/prod/manifest.json ./prod-state/manifest.json`.
   - `dbt deps`.
   - `dbt build --select state:modified+ --defer --state ./prod-state --target ci`.
   - On success, post a PR comment listing the affected models. On failure, fail the job.
   - Cleanup step: `dbt run-operation drop_old_ci_schemas`.
3. **Profiles**: `profiles.yml` in the repo with `ci` target schema like `ci_${{ github.run_id }}`. Credentials from secrets.
4. **Branch protection**: require the CI check to pass before merging.

The whole thing fits in ~50 lines of YAML.

### **Q7. [Advanced] Your CI is taking 45 minutes for small PRs. How do you debug and fix?**

**Model answer:** Likely culprits:
1. **Not using Slim CI.** Check the workflow — is `--select state:modified+` present? If not, add it; you're rebuilding everything.
2. **`--defer` missing.** Without it, every dependency rebuilds. Add `--defer --state ./prod-state`.
3. **Tests run on the entire project.** A PR shouldn't re-test unchanged models. Slim CI handles this — confirm with `dbt list --select state:modified+`.
4. **Heavy seeds reloaded every PR.** Seeds rarely change; consider `dbt seed --select state:modified --state ./prod-state`.
5. **Slow individual models.** `jq '.results | sort_by(-.execution_time) | .[0:5]' run_results.json` to find the heavies; consider sampling them in CI via `target.name == 'ci'` conditional.

The fastest CI pipelines I've seen do incremental dbt builds in 2–4 minutes for a typical PR; 45 minutes means something is wrong, not that the project is too big.

---

## **12. GOTCHAS**

- **`--state` requires a directory containing `manifest.json`**, not just the file. `--state ./prod-state` looks for `./prod-state/manifest.json`.
- **`state:modified+` doesn't catch macro changes** unless the macro is referenced from a model whose compiled output changes. A pure-macro PR may select 0 models — manually trigger a full build for those.
- **`--defer` requires the deferred-to relation to actually exist in prod.** First-time deploy of a new model has nothing to defer to; use `--full-refresh` or initial seed manually.
- **`dbt parse` is fast but shallow** — catches Jinja syntax, not SQL syntax. SQL errors only surface on actual run.
- **CI schemas pile up** if you don't drop them. Schedule cleanup or use a TTL in your warehouse.
- **`dbt deps` in CI without `--lock`** can pull a different version than dev. Use `package-lock.yml` (dbt 1.7+) for reproducibility.
- **`run_results.json` only contains the last run.** A `dbt run` followed by `dbt test` overwrites the run results from the run. Use `dbt build` to get model + test results in one artifact.
- **`--debug` flag is verbose.** Pipe to a file: `dbt run --debug 2> debug.log`.
- **CI failing with "could not find profile"** — `DBT_PROFILES_DIR` not set. Common solution: commit a `profiles.yml` in the repo at `./profiles.yml` and set `DBT_PROFILES_DIR=./`.
- **Slim CI false negatives** — if a downstream model has runtime-only data dependencies (e.g., reads from a runtime-injected var), state comparison can miss the impact. Tag those models and force-include with `--select state:modified+ tag:always_run`.

---

## **NEXT STEP**

You've covered Week 3 production-grade. Now switch gears to interview talking points and adoption arguments.

Go to [`12_why_dbt_wins_2026.md`](12_why_dbt_wins_2026.md).
