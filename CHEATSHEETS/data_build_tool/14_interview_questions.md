# **14 — 30 Interview Questions, Ranked & Tagged**

> **Goal:** A drill deck. Each question is tagged by difficulty, with a complete model answer and a cross-reference to the file with deeper context. Talk through them out loud — interviews reward fluency, not memorization.

---

## **HOW TO USE THIS FILE**

1. Cover the answers, read each question, talk through your answer in 30–90 seconds.
2. Compare to the model answer. Note gaps.
3. Re-read the cross-referenced file for any answer you missed.
4. Repeat 2–3 times across the week before the interview.

Questions are grouped by topic, not difficulty. Tags:
- **[F]** Foundational — should answer in <30 sec
- **[I]** Intermediate — 60–90 sec, some depth
- **[A]** Advanced — 90+ sec, system thinking, trade-offs

---

## **PART A — FUNDAMENTALS (Q1–Q6)**

### **Q1. [F] What is dbt?**

dbt is an open-source transformation framework that lets you write data transformations as SQL, with software-engineering rituals: version control via Git, declarative tests, auto-generated documentation, lineage from a DAG, and modular SQL via Jinja templating. It's the "T" in ELT — it sits between raw warehouse data and consumption layers (BI, ML), and compiles to native warehouse SQL with no engine of its own. Comes in two flavors: dbt Core (open-source CLI) and dbt Cloud (commercial SaaS on top).

**Cross-ref:** `00_start_here.md`

---

### **Q2. [F] What problem does dbt solve?**

The pre-dbt analytics SQL world had no version control (`query_v3_FINAL.sql`), no tests (bugs discovered by stakeholders), no docs (tribal knowledge), and no lineage tracking. dbt brings all four into one tool: SQL files in Git, declarative tests on every model, an auto-generated docs site with the DAG, and `ref()` calls that make dependencies explicit. The infographic phrases it as "version chaos → version controlled, dashboards-as-detector → CI tests catch bugs, tribal knowledge → auto docs."

**Cross-ref:** `00_start_here.md` §2; `12_why_dbt_wins_2026.md` §4

---

### **Q3. [F] What's the difference between dbt Core and dbt Cloud?**

dbt Core is the open-source CLI — compiler, runner, test framework, docs generator. Apache 2.0, free, runs anywhere you have Python. dbt Cloud is the commercial SaaS layer on top — hosted IDE, scheduler, CI integration, semantic layer, monitoring, alerts. Pick Core if you have your own orchestration and CI; pick Cloud if you don't and want the hosted experience.

**Cross-ref:** `00_start_here.md` §5

---

### **Q4. [F] What are the core dbt concepts I should know?**

Six: **Models** (SQL files that become tables/views), **Sources** (declared raw tables), **Tests** (declarative data quality assertions), **Macros** (reusable Jinja+SQL functions), **Snapshots** (SCD2 history captures), **Seeds** (small CSVs loaded as tables). Plus the foundational ideas: **`ref()` and `source()`** for the DAG, **materializations** (`view`/`table`/`ephemeral`/`incremental`) for deployment strategy, and **Jinja** for templating.

**Cross-ref:** `00_start_here.md` §6

---

### **Q5. [F] What's the difference between `ref()` and `source()`?**

`ref('model_name')` references another dbt model in the same project (or another project in dbt Mesh). `source('source_name', 'table_name')` references a raw table in the warehouse that dbt didn't create — sources have to be declared in a `sources:` YAML block first. Both create dependency edges in the DAG. Both resolve at compile time to fully-qualified warehouse names. Use `source()` only at the staging layer (one source-consuming staging model per source table); use `ref()` everywhere else. Hardcoding table names in SQL is invisible to dbt's parser and breaks the DAG.

**Cross-ref:** `03_sources_and_ref.md`

---

### **Q6. [F] What's the difference between `dbt run` and `dbt build`?**

`dbt run` runs models only. `dbt build` runs models, tests, snapshots, and seeds in DAG order — and on a test failure for an upstream model, it skips downstream models and tests. `build` is the production-correct command because it stops bad data from propagating. `run` followed by `test` runs all models first regardless, then all tests; downstream models build even on upstream test failure.

**Cross-ref:** `04_tests.md`; `11_cicd_and_debugging.md`

---

## **PART B — MATERIALIZATIONS & MODELS (Q7–Q12)**

### **Q7. [F] What are the four built-in materializations?**

`view`, `table`, `ephemeral`, and `incremental`. `view` and `table` map to `CREATE OR REPLACE VIEW/TABLE` DDL — view is cheap to build, expensive on every read; table is the opposite. `ephemeral` doesn't physicalize — it's inlined as a CTE in downstream models. `incremental` builds the full table on first run, then on subsequent runs only processes new rows and merges them in. Default is usually `view`; escalate to `table` for heavy aggregations consumed often, `incremental` for huge fact tables.

**Cross-ref:** `02_models_and_materializations.md`

---

### **Q8. [I] How do you choose between view, table, and incremental?**

Math:
- **View cost** = compute(SELECT) × reads_per_run.
- **Table cost** = compute(build) once per run + cheap reads.
- **Incremental cost** = compute(build delta) once per run + cheap reads.

If reads are infrequent and freshness matters, `view`. If reads are frequent and data is small enough to rebuild quickly, `table`. If the table is huge but appends/merges cleanly, `incremental`. The other consideration: complexity. Incremental adds knobs (unique_key, lookback, on_schema_change) you have to maintain. Don't go incremental for a table that takes 30 seconds to fully rebuild.

**Cross-ref:** `02_models_and_materializations.md` §10

---

### **Q9. [I] What does `is_incremental()` return and when?**

`true` only when all three are true: (1) the model's materialization is `incremental`, (2) the target table already exists, (3) `--full-refresh` was not passed. So on the first run it returns `false` (target doesn't exist), letting dbt build the full table. On subsequent runs it returns `true`, activating the WHERE filter for new rows. With `--full-refresh` it returns `false`, forcing a rebuild.

**Cross-ref:** `09_incremental_deep_dive.md` §1

---

### **Q10. [I] What are the incremental strategies and when do you pick each?**

`merge` — real `MERGE` SQL on Snowflake/BigQuery/Databricks/Postgres15+/DuckDB; default for warehouses that support it. `delete+insert` — for warehouses without MERGE (Redshift, older Postgres). `append` — pure inserts, no dedup; for clickstream/event data with upstream idempotency. `insert_overwrite` — replace partitions atomically; for time-partitioned tables on BigQuery/Spark. `microbatch` (1.9+) — time-bounded micro-batches with declarative lookback. Pick `merge` by default; `append` for events; `insert_overwrite` for partitioned aggregates; `microbatch` for new code where you want clean late-arriving handling.

**Cross-ref:** `09_incremental_deep_dive.md` §4

---

### **Q11. [A] Walk me through what dbt does internally on a `merge` incremental run.**

Three steps: (1) Build a temp table with the filtered SELECT (the "delta"). (2) Issue `MERGE INTO target USING temp ON unique_key WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. (3) Drop the temp table. The temp-table approach exists so the SELECT (which can include heavy joins/CTEs) runs once and the warehouse can plan the MERGE against a known relation. dbt writes the rendered MERGE to `target/run/.../<model>.sql` so you can inspect it. The MERGE is atomic on warehouses that support it.

**Cross-ref:** `09_incremental_deep_dive.md` §3

---

### **Q12. [A] Your incremental model is producing duplicates. What's likely wrong?**

Three common causes: (1) **`unique_key` isn't actually unique** — multiple rows per key in source. MERGE picks one nondeterministically; others are inserted as new rows. Fix: add `unique` test, find the real composite key, or pre-aggregate. (2) **`incremental_strategy='append'` with non-idempotent ingestion** — same source row arrives twice. Fix: switch to merge with a real unique_key, or filter on `_ingested_at`. (3) **Schema sync that dropped the unique_key column.** Debug with `select unique_key, count(*) from {{ this }} group by 1 having count(*) > 1`, then trace `_dbt_loaded_at` to find when the dup appeared.

**Cross-ref:** `09_incremental_deep_dive.md` §13

---

## **PART C — TESTS & QUALITY (Q13–Q16)**

### **Q13. [F] What are the four built-in generic tests?**

`unique`, `not_null`, `accepted_values`, `relationships`. `unique` checks no duplicate non-null values. `not_null` checks no nulls. `accepted_values` checks every value is in a given list. `relationships` checks every value exists in another model's column — that's the foreign key test. All four compile to a SELECT that returns failing rows; zero rows = pass.

**Cross-ref:** `04_tests.md` §4

---

### **Q14. [I] What's the difference between a generic test, singular test, and custom generic test?**

A **generic test** is a parameterized macro applied via YAML to many columns/models — `unique`, `not_null`, custom ones you define, or imports from `dbt_utils`/`dbt_expectations`. A **singular test** is a one-off `.sql` file under `tests/` containing a SELECT that returns zero rows on pass; used for cross-model invariants. A **custom generic test** is a `{% test name(...) %}` macro you write to encode an org-specific reusable pattern (valid email, future date, value in range), applied via YAML like the built-ins. Use generic for reusable, singular for one-off.

**Cross-ref:** `04_tests.md` §3, §5, §6

---

### **Q15. [I] When would you use `severity: warn` instead of error?**

When a test is informational rather than blocking. Default is `error`, which fails `dbt build` and (in CI) blocks merges. `warn` produces a WARNING but lets the run continue. Useful for: aspirational tests on freshly-onboarded sources, tolerance tests during migration, or the `error_if: ">N"` threshold pattern where you warn for 1–10 issues but error past 100. The honest rule: default to error, demote to warn only when proven necessary, and never let `warn` become noise.

**Cross-ref:** `04_tests.md` §8

---

### **Q16. [A] How would you implement a test that verifies the sum of `fct_orders.amount` equals the sum of `stg_orders.amount`?**

Singular test. `tests/orders_amounts_match.sql`:

```sql
with f as (select sum(amount) as total from {{ ref('fct_orders') }}),
     s as (select sum(amount) as total from {{ ref('stg_orders') }})
select f.total, s.total
from f, s
where abs(f.total - s.total) > 0.01
```

Returns one row if invariant fails, zero otherwise. The `abs( … ) > 0.01` tolerates penny-level floating-point rounding. Singular tests are perfect for invariants spanning multiple models because no column-level generic test can express "compare aggregations across two tables."

**Cross-ref:** `04_tests.md` §5

---

## **PART D — JINJA & MACROS (Q17–Q20)**

### **Q17. [F] Why does dbt use Jinja?**

To add control flow, variables, functions, and macros to SQL — things SQL itself doesn't have. Jinja runs at compile time, before SQL hits the warehouse, so the warehouse never sees `{% for %}` or `{{ }}`. End result is plain warehouse SQL. dbt-specific functions like `ref()`, `source()`, `config()`, `is_incremental()` are all Jinja macros provided by dbt. Jinja is the templating layer that turns SQL into modular, reusable, environment-aware code.

**Cross-ref:** `05_macros_and_jinja.md` §1, §2

---

### **Q18. [F] What's the difference between `{{ }}` and `{% %}`?**

`{{ }}` evaluates an expression and **renders the result as text** in the output. `{% %}` is a **control statement** — `if`, `for`, `set`, `macro` — that doesn't produce output, just controls flow. So `{{ ref('x') }}` outputs the table name; `{% if x %}…{% endif %}` controls whether a block renders. `{# … #}` is a comment, removed at compile.

**Cross-ref:** `05_macros_and_jinja.md` §3

---

### **Q19. [I] When would you write a custom macro?**

When the same SQL pattern appears 3+ times across the project. Examples: `safe_divide` to protect against div-by-zero; `cents_to_dollars` to normalize monetary columns; `tenant_filter` to inject multi-tenant predicates. Macros take args and render SQL; called with `{{ macro_name(args) }}`. Before writing one, check `dbt_utils` and `dbt_expectations` — many common patterns are already there. The trigger is DRY violation, not abstract reuse.

**Cross-ref:** `05_macros_and_jinja.md` §6

---

### **Q20. [A] How does `adapter.dispatch` work?**

`adapter.dispatch('name')` looks up a macro using a fallback chain: `<adapter>__name` (e.g., `snowflake__name`) → `<package>__name` → `default__name`. Used for macros whose implementation differs per warehouse — date functions, JSON path syntax, regex syntax. The pattern: define a public macro that calls `adapter.dispatch`, then provide one or more `<adapter>__macro` implementations. This is how `dbt_utils` ships one macro that works on every adapter — each adapter contributes its own implementation, dispatch routes the call.

**Cross-ref:** `05_macros_and_jinja.md` §9

---

## **PART E — STRUCTURE, SOURCES, SNAPSHOTS, SEEDS (Q21–Q25)**

### **Q21. [F] Walk me through the layered structure of a dbt project.**

Three layers. **Staging** (`models/staging/`) — one model per source table, light cleanup, named `stg_<source>__<table>`, materialized as views. **Intermediate** (`models/intermediate/`) — optional helpers, `int_<entity>_<verb>`, ephemeral or view. **Marts** (`models/marts/`) — business-grain, subdivided by domain (`core/`, `finance/`, `marketing/`), `dim_<entity>` and `fct_<entity>`, materialized as tables or incrementals. Refs flow top-to-bottom only. Source-direct refs from marts are forbidden.

**Cross-ref:** `08_project_structure.md`

---

### **Q22. [F] What's a snapshot?**

A snapshot is a dbt-maintained table that captures point-in-time history of a source table — SCD Type 2. Each row has `dbt_valid_from` / `dbt_valid_to` columns. When source data changes, dbt expires the existing snapshot row and inserts a new version. Query for current state with `where dbt_valid_to is null`; for historical state with date predicates against the validity window. Two strategies: `timestamp` (compares an `updated_at` column, cheaper) and `check` (hashes listed columns, used when no reliable updated_at).

**Cross-ref:** `06_snapshots_and_seeds.md` §2

---

### **Q23. [F] What's a seed and when shouldn't you use one?**

A seed is a CSV file under `seeds/` that dbt loads into the warehouse via `dbt seed`. Used for small, slow-moving reference data — country codes, status labels, holiday calendars — that belongs in the same Git repo as the dbt code. Seeds are DAG nodes; reference with `ref('seed_name')`. **Don't use seeds** for large datasets (>10k rows), frequently changing data (weekly+), sensitive data (PII), or anything that should really be ingested. Code repos aren't data warehouses.

**Cross-ref:** `06_snapshots_and_seeds.md` §3

---

### **Q24. [I] How does dbt's parse phase work, and why does it matter?**

Two phases: parse and run. In **parse**, dbt walks every file, renders Jinja in a discovery mode just to extract `ref()`/`source()` calls and configs, builds the DAG, writes `target/manifest.json`. In **run**, it executes nodes in topological order. Why it matters: parse errors (unresolved refs, missing sources, syntax) surface before any SQL runs. `dbt parse` is fast, no warehouse calls, perfect for CI sanity checks. Partial parsing optimizes by re-parsing only changed files between runs.

**Cross-ref:** `03_sources_and_ref.md` §3

---

### **Q25. [A] What's `dbt source freshness` and how would you wire it into production?**

`dbt source freshness` queries the `loaded_at_field` of every source declared with freshness thresholds and reports any older than `warn_after` / `error_after`. In production: run it on a separate schedule from model builds — every 30–60 min for critical sources. Output is `target/sources.json`, integrate with PagerDuty/Slack/OpsGenie. Freshness checks are independent of the model run, so a stale source can alert without blocking transformations and vice versa. Sources need a `loaded_at_field` column for this to work — usually an ingestion timestamp.

**Cross-ref:** `03_sources_and_ref.md` §10; `04_tests.md` §9

---

## **PART F — CI/CD, DOCS, PRODUCTION (Q26–Q30)**

### **Q26. [F] What is Slim CI?**

Slim CI is the pattern of building only what changed in a PR, not the whole project. Uses `state:modified+` to select models whose code differs from a saved prod manifest, plus their downstream. Combined with `--defer --state`, unchanged dependencies reference prod's tables instead of being rebuilt. Result: PR builds in minutes instead of hours. Required: `target/manifest.json` from a prior prod run, persisted somewhere CI can fetch (S3/GCS).

**Cross-ref:** `11_cicd_and_debugging.md` §3

---

### **Q27. [F] What's `manifest.json` and why does it matter?**

`manifest.json` is dbt's DAG dump — every model, source, test, snapshot, seed, exposure with their configs, compiled SQL, and dependencies. Written to `target/` after every dbt invocation. Used by `dbt docs serve` (renders the docs site), `state:modified+` (compares two manifests for Slim CI), and external tools (DataHub, Elementary, OpenLineage). In production, persist it to S3/GCS after every run so CI can use it for state-based selection.

**Cross-ref:** `11_cicd_and_debugging.md` §4

---

### **Q28. [I] How would you investigate a model that failed in prod last night?**

(1) Pull `target/run_results.json` from the failed run (assuming it's persisted to S3). (2) `jq '.results[] | select(.status != "success")' run_results.json` to find the failing model and error message. (3) Pull the compiled SQL from `target/compiled/`, paste it into a SQL client, reproduce the error against prod data. (4) If that's not enough, re-run the model with `--log-level debug` and tail the logs. The whole loop: read run_results → read compiled SQL → reproduce in SQL client → fix.

**Cross-ref:** `11_cicd_and_debugging.md` §5

---

### **Q29. [I] How would you keep dbt docs in sync with the SQL?**

A few mechanisms: (1) **`dbt-osmosis`** — open-source CLI that propagates column descriptions through the DAG and inserts placeholder YAML for new columns; runs in CI. (2) **`dbt-checkpoint`** — pre-commit hooks that fail if a model has columns missing from YAML. (3) **Custom CI step** — diff `manifest.json` columns against YAML, fail on mismatch. (4) **Convention** — every PR that adds a column also updates YAML, enforced by code review. Also enable `persist_docs: { relation: true, columns: true }` so descriptions are written as warehouse-native column comments.

**Cross-ref:** `07_documentation_and_lineage.md` §10, §13

---

### **Q30. [A] If you had to argue dbt's design trade-offs to a skeptical engineer, what would you say?**

Five trade-offs worth naming honestly:
1. **Compile-time vs runtime** — Jinja gives templating but no real runtime control flow. "Iterate over rows and call an API per row" is impossible; that's a Python job.
2. **No engine** — scales as the warehouse scales but inherits its constraints. No Spark-style data partitioning tricks.
3. **Stateless models** — idempotent and clean, but snapshots and incrementals are escape hatches with rough edges (snapshot rebuild path, incremental schema evolution).
4. **SQL-first** — easy hiring but awkward for ML feature engineering, graph algorithms, complex stateful logic.
5. **Convention over configuration** — fast onboarding but edge cases (multi-source models, cross-project refs, custom materializations) require fighting the conventions.

The framing: each was the right call for the use case dbt won, but each has a downside that bites when you push past it. dbt's not perfect — it's the right tool for analytics SQL on warehouses, and its dominance reflects right-tool-right-time more than technical superiority.

**Cross-ref:** `12_why_dbt_wins_2026.md` §6

---

## **BONUS — RAPID-FIRE Q&A**

These should take <15 sec each. Drill them last.

| Q | A |
|---|---|
| What's `{{ this }}`? | The current model's fully-qualified relation name |
| What's `{{ target.name }}`? | The active target (`dev`, `prod`, `ci`) |
| Materialization for one row per customer? | Probably `table` |
| Materialization for a 10B-row events table? | `incremental` |
| How do you load a CSV into the warehouse? | `dbt seed` |
| Where do passwords go? | `env_var()` in `profiles.yml`, never in code |
| What does `dbt deps` do? | Install packages from `packages.yml` |
| What's the post_hook variable for the model's own name? | `{{ this }}` |
| What's the difference between tags and meta? | Tags filter selectors; meta is metadata for tooling |
| What's `--full-refresh` for? | Force-rebuild incrementals from scratch |
| What command runs models + tests + snapshots + seeds? | `dbt build` |
| What's a "Slim CI"? | Build only models changed by a PR |
| What's `dbt list --select state:modified+ --state ./prod` print? | Models changed since the saved manifest, plus downstream |
| What format is `run_results.json`? | JSON; one entry per node with status, timing, message |
| What's the package every project uses? | `dbt_utils` |
| What's a doc block? | `{% docs name %}…{% enddocs %}` — reusable description |
| Is dbt SQL or Python? | SQL with Jinja templating; supports Python models in newer versions |
| What's `dbt show`? | Print rows from a model without materializing |
| What does `severity: warn` do? | Test failures log warning instead of failing the run |
| Where does `profiles.yml` live? | `~/.dbt/profiles.yml` (configurable via `--profiles-dir`) |
| What's `is_incremental()` false on the first run? | Because the target table doesn't exist yet |

---

## **HOW TO DRILL THIS DECK**

**Day 1 (Mon):** Read all 30. Pause at each, attempt verbal answer.
**Day 2:** Q1–Q15 again, more focus on weak spots.
**Day 3:** Q16–Q30 again.
**Day 4:** Mix — randomized order, all 30, no peeking.
**Day 5:** Bonus rapid-fire + the 5 you keep getting wrong.
**Day before interview:** Skim cheat sheet, re-read any answer you're unsure on.

If you can answer 27/30 confidently, you'll dominate any dbt-themed interview round.

---

## **NEXT STEP**

Sanity-check your vocabulary.

Go to [`15_glossary.md`](15_glossary.md).
