# **15 — Glossary**

> Every dbt term you'll encounter, defined precisely. When in doubt, look it up.

---

## **A**

**Adapter** — A Python package that translates dbt's compiled SQL into a specific warehouse's dialect and executes it. Examples: `dbt-snowflake`, `dbt-bigquery`, `dbt-duckdb`. Without an adapter, dbt-core has no warehouse to talk to.

**`adapter.dispatch`** — Jinja function that resolves a macro to a warehouse-specific implementation. Looks up `<adapter>__macro_name`, falls back to `default__macro_name`. How `dbt_utils` ships one macro that works everywhere.

**Analyses** — SQL files under `analyses/` that compile to pure SQL but don't run. Used for ad-hoc queries you want versioned and templated.

**Analytics Engineer** — Job title coined by dbt Labs around 2020. The role between Data Analyst and Data Engineer; primary tool is dbt.

**`append`** (incremental strategy) — INSERT-only, no dedup. Used for append-only event streams. Risk: duplicates if upstream re-ingests.

**Artifact** — JSON files dbt writes to `target/` after every command. Includes `manifest.json`, `run_results.json`, `catalog.json`, `sources.json`. Consumed by docs site, CI, and observability tools.

**`accepted_values`** — Built-in generic test that fails if any value in a column is outside a given list. Used for enums.

---

## **B**

**Build** — Running models, tests, snapshots, and seeds together in DAG order with `dbt build`. The production-correct command.

**BigQuery** — One of the four major warehouses dbt targets. Adapter: `dbt-bigquery`.

**`bigquery__macro_name`** — Adapter-specific override pattern; called by `adapter.dispatch`.

---

## **C**

**`catalog.json`** — Artifact written by `dbt docs generate`. Contains warehouse-derived metadata (column types, table sizes) read from `information_schema`.

**`check` strategy** (snapshot) — SCD2 strategy that hashes a list of columns each run to detect changes. Used when the source has no reliable updated_at column.

**CLI** — Command-line interface. `dbt-core` is fundamentally a CLI; `dbt run`, `dbt test`, etc.

**`cluster_by`** — Config that sets the warehouse-side clustering key (Snowflake) or clustering columns (BigQuery). Performance hint, doesn't affect logic.

**Compile** — Phase where dbt renders Jinja into pure SQL. Outputs to `target/compiled/`. `dbt compile` triggers it without running.

**Compiled SQL** — Pure SQL after Jinja resolution. The thing the warehouse executes.

**Composite key** — A `unique_key` of multiple columns (`unique_key=['order_id', 'product_id']`). Generates `MERGE … ON a.order_id = b.order_id AND a.product_id = b.product_id`.

**Contract** (model contract) — dbt 1.5+ feature where a model declares its column types and constraints; `dbt run` enforces them at build time. Stronger than tests.

**Coalesce conference** — Annual dbt Labs conference (since 2020). Major venue for the analytics-engineering community.

**`config()`** — Jinja function that sets compile-time config inside a model file. `{{ config(materialized='table') }}`.

**Custom generic test** — A `{% test name(model, column_name) %}…{% endtest %}` macro you write to encode a reusable assertion.

---

## **D**

**DAG** — Directed Acyclic Graph. The dependency graph of models built from `ref()` and `source()` calls. dbt runs nodes in topological order.

**`data_tests:`** — YAML key for declaring tests on a model or column (dbt 1.8+). Older versions used `tests:`.

**`databricks` adapter** — `dbt-databricks` for running dbt on Databricks Lakehouse. Distinct from `dbt-spark`.

**`dbt build`** — Runs models + tests + snapshots + seeds in DAG order. Stops downstream on test failure.

**`dbt-checkpoint`** — Pre-commit hooks for dbt projects (linting, completeness checks).

**`dbt clean`** — Deletes `target/` and `dbt_packages/`.

**`dbt Cloud`** — Commercial SaaS layer on top of dbt-core. Hosted IDE, scheduler, CI, monitoring, semantic layer.

**`dbt-core`** — The open-source CLI. Apache 2.0.

**`dbt debug`** — Validates `profiles.yml`, `dbt_project.yml`, and warehouse connection.

**`dbt deps`** — Installs packages from `packages.yml`.

**`dbt docs generate`** — Builds `manifest.json`, `catalog.json`, and the static docs bundle.

**`dbt docs serve`** — Serves the docs site on `:8080` (configurable).

**`dbt-expectations`** — Community package porting Great Expectations' assertions to dbt.

**`dbt init`** — Scaffolds a new dbt project.

**`dbt-labs`** — The company behind dbt. Founded 2016 as Fishtown Analytics.

**`dbt list`** — Lists nodes matching a selector. Useful for CI debugging.

**`dbt Mesh`** — dbt 1.6+ feature for composing multiple dbt projects with cross-project refs and access controls.

**`dbt parse`** — Parses files only, no run. Catches syntax and unresolved refs.

**`dbt-osmosis`** — Open-source tool for propagating column descriptions through dbt projects.

**`dbt run`** — Runs models only.

**`dbt run-operation`** — Run a macro on demand. Useful for one-off tasks like dropping CI schemas.

**`dbt show`** — Prints rows from a model without materializing it (1.6+).

**`dbt source freshness`** — Queries declared sources for staleness, reports breaches of `warn_after`/`error_after`.

**`dbt test`** — Runs tests only.

**`dbt-utils`** — The most-used dbt package. Surrogate keys, advanced tests, helper macros.

**`dbt_project.yml`** — The project config file. Lives in the repo. Defines name, paths, models config, vars, hooks.

**`dbt_scd_id`** — Column auto-added to snapshots; surrogate key hash of unique_key + dbt_valid_from.

**`dbt_valid_from` / `dbt_valid_to`** — Snapshot columns marking each row's validity window. `dbt_valid_to` is NULL for current rows.

**Defer** — `--defer --state ./prod-state` flag pair that lets a CI run reference prod's tables for unchanged dependencies. Required for Slim CI.

**`delete+insert`** (incremental strategy) — Delete by unique_key, then INSERT. Used on warehouses without MERGE.

**Description** — A YAML or doc-block string attached to a model, source, column, or other node. Renders in the docs site.

**Dim** — Dimension table. Naming convention: `dim_<entity>` (e.g., `dim_customers`).

**Doc block** — A `{% docs name %}…{% enddocs %}` markdown block. Reusable description, referenced via `{{ doc('name') }}`.

**DuckDB** — In-process OLAP database. Adapter: `dbt-duckdb`. Used in this curriculum for hands-on exercises.

---

## **E**

**Elementary** — Open-source dbt observability tool that consumes artifacts and visualizes run history.

**`enabled`** — Config that disables a model entirely (`enabled: false`). Removed from the DAG.

**`env_var()`** — Jinja function reading OS environment variables. Used for secrets in `profiles.yml`.

**`ephemeral`** — Materialization that doesn't physicalize — inlined as a CTE in downstream models.

**ETL** vs **ELT** — Extract-Transform-Load (legacy) vs Extract-Load-Transform (dbt's world). dbt does the T after L.

**Exposure** — A DAG node representing a downstream consumer (dashboard, notebook, ML pipeline). Declared in YAML; appears in the docs site as a leaf node.

---

## **F**

**Fct** — Fact table. Naming convention: `fct_<entity>` (e.g., `fct_orders`).

**`fct_orders`** — Conventional name for an order fact table.

**Fishtown Analytics** — Original name of dbt Labs (founded 2016).

**Freshness** — Source freshness check; queries the `loaded_at_field` column to determine staleness.

**`--full-refresh`** — CLI flag forcing incremental models to rebuild from scratch.

---

## **G**

**`generate_schema_name`** — Built-in macro that determines a model's warehouse schema. Often overridden to use literal schema names in prod and prefixed schemas in dev.

**Generic test** — Parameterized test macro applied via YAML. `unique`, `not_null`, custom ones.

**Grain** — The level of detail in a table. "One row per customer" or "one row per order line." A model description should always state the grain.

**Granted (permissions)** — Often handled via post-hooks in dbt: `+post-hook: GRANT SELECT ON {{ this }} TO ROLE analyst`.

---

## **H**

**Hook** — SQL that runs around a model or run. `pre_hook` / `post_hook` are per-model; `on-run-start` / `on-run-end` are per-invocation.

**High-water mark** — The maximum value of a column (often a timestamp) tracked by an incremental model to filter for new rows.

---

## **I**

**Idempotent** — A run that produces the same result when repeated. dbt models are designed to be idempotent; a `dbt run` with the same input produces the same output.

**Incremental** — Materialization that processes only new/changed rows after the first build. Critical for big fact tables.

**`incremental_strategy`** — Config selecting how to merge new rows: `merge`, `delete+insert`, `append`, `insert_overwrite`, `microbatch`.

**`insert_overwrite`** (incremental strategy) — Replace partitions atomically. Used on time-partitioned tables.

**Intermediate model** — Optional middle layer. Naming: `int_<entity>_<verb>`. Materialized as ephemeral or view.

**`invalidate_hard_deletes`** — Snapshot config; if true, rows that disappear from source are expired (`dbt_valid_to = now()`).

**`invocation_id`** — Per-run UUID accessible in Jinja. Useful for audit logging.

**`is_incremental()`** — Jinja function returning true only when the run is an incremental update (target exists, materialization is incremental, no `--full-refresh`).

---

## **J**

**Jinja** — Python templating language used by dbt. Adds control flow, variables, functions, macros to SQL.

---

## **L**

**Lineage** — The dependency graph from `ref()`/`source()` calls. Visualized in `dbt docs serve`.

**Lookback window** — A pattern in incremental models: filter on `where x >= max(x) - interval N days` to handle late-arriving data.

---

## **M**

**Macro** — A reusable Jinja+SQL function defined with `{% macro name() %}…{% endmacro %}`. Lives under `macros/`.

**`manifest.json`** — Artifact representing the full DAG. Used for docs, Slim CI, and external integrations.

**Mart** — Business-grain layer of dbt models. Subdivided by domain (`core/`, `finance/`, `marketing/`).

**Materialization** — How a model becomes physical: `view`, `table`, `ephemeral`, `incremental`. Controlled by `{{ config(materialized='...') }}`.

**Medallion architecture** — Bronze/Silver/Gold pattern from Databricks. Maps to staging/intermediate/marts in dbt.

**`merge`** (incremental strategy) — `MERGE` SQL for upsert. Default on Snowflake, BigQuery, Databricks, Postgres ≥15, DuckDB.

**`microbatch`** (incremental strategy) — Time-bounded micro-batches with declarative lookback. dbt 1.9+.

**Model** — A `.sql` file under `models/` containing one SELECT. Becomes a table or view in the warehouse.

---

## **N**

**Node** — A first-class entity in the DAG: model, source, test, snapshot, seed, exposure, analysis.

**`not_null`** — Built-in generic test failing if any null exists in the column.

---

## **O**

**`on-run-start` / `on-run-end`** — Hooks running once per dbt invocation.

**`on_schema_change`** — Incremental config controlling behavior when columns change: `ignore`, `fail`, `append_new_columns`, `sync_all_columns`.

**OpenLineage** — Open-standard for emitting lineage events. dbt has integration tools that emit OpenLineage from manifests.

**Operation** — Custom code run via `dbt run-operation <macro>`.

---

## **P**

**Package** — A reusable dbt project distributed via `packages.yml`. `dbt_utils` is the canonical example.

**`packages.yml`** — File listing dbt package dependencies. Installed by `dbt deps` into `dbt_packages/`.

**Parse** — Phase where dbt walks files and builds the manifest. Cheap; no warehouse access for most projects.

**Partial parsing** — Optimization where dbt re-parses only changed files between runs. On by default.

**`partition_by`** — Config for partitioned tables on BigQuery and Spark.

**`persist_docs`** — Config that writes YAML descriptions as warehouse-native column comments. `{relation: true, columns: true}`.

**Post-hook / pre-hook** — Per-model hooks running after / before the model's main statement.

**`profiles.yml`** — Per-user warehouse credentials. Lives in `~/.dbt/`. NEVER committed.

---

## **Q**

(none)

---

## **R**

**`ref('model_name')`** — The function that creates a dbt-internal dependency edge. Resolves at compile to a fully-qualified warehouse name.

**Relation** — dbt's abstraction for "a table or view in the warehouse." `ref()` and `source()` return Relation objects.

**`relationships`** — Built-in generic test asserting every value in a column exists in another model's column. The foreign-key test.

**`run_query`** — Jinja function that executes SQL during compile and returns results. Gate with `{% if execute %}` to avoid running during parse.

**`run_results.json`** — Artifact written after each command. Status, timing, error messages per node.

**`run_started_at`** — Jinja variable; timestamp of run start.

---

## **S**

**SCD** — Slowly Changing Dimension. dbt snapshots implement SCD Type 2.

**Schema** (warehouse) — A namespace within a database. Models are written to schemas.

**Schema (YAML)** — `schema.yml` style files describing models, columns, tests. Modern dbt accepts any `.yml` filename.

**Seed** — Static CSV under `seeds/` loaded as a warehouse table by `dbt seed`.

**Selector** — CLI argument like `--select tag:nightly` or `state:modified+`.

**Semantic Layer** — dbt's metric definition layer (formerly MetricFlow). Separate from docs.

**Singular test** — A one-off `.sql` file under `tests/` containing a SELECT that returns zero rows on pass.

**Slim CI** — Pattern of building only changed models in CI using `state:modified+` and `--defer`.

**Snapshot** — A node type that captures SCD2 history of a source. Run via `dbt snapshot`.

**Snowflake** — One of the four major warehouses. Adapter: `dbt-snowflake`.

**Source** — A declared raw warehouse table. Referenced in models via `source('source_name', 'table_name')`.

**`sources.json`** — Artifact written by `dbt source freshness`.

**SQLMesh** — Newer competitor to dbt with virtual environments and stronger state management.

**`stg_<source>__<table>`** — Naming convention for staging models.

**`store_failures`** — Test config that persists failing rows to a warehouse table for inspection.

**Surrogate key** — Computed primary key from one or more source columns. `dbt_utils.generate_surrogate_key(['col1','col2'])`.

**`sync_all_columns`** — `on_schema_change` value that adds new columns and drops removed ones. Risky.

---

## **T**

**Table** — Materialization that writes the SELECT to a physical table.

**Tag** — A label attached to a model via config. Filterable on the CLI: `--select tag:nightly`.

**Target** — A named environment in `profiles.yml`: `dev`, `ci`, `prod`. Switched via `--target`.

**`target.name` / `target.schema` / `target.database` / `target.type`** — Jinja variables exposing the active target.

**`target/`** — The output directory dbt writes to. Compiled SQL, run results, manifest, catalog, logs. Gitignored.

**Templater** — Casual term for "Jinja preprocessor." dbt's role isn't *just* templating, but Jinja is the templating layer.

**`tests:`** — Older YAML key for declaring tests (pre-1.8). Still works; `data_tests:` is preferred.

**Test severity** — `error` (default) fails the run; `warn` logs a warning.

**`this`** — Jinja variable representing the current model's relation. Used in incremental models.

**`timestamp` strategy** (snapshot) — SCD2 strategy comparing an `updated_at` column. Cheaper than `check`.

**Threading** — Parallelism level set in `profiles.yml`. `threads: 8` runs up to 8 nodes concurrently.

**Topological order** — DAG ordering where every node is run after its dependencies. dbt runs in topological order.

---

## **U**

**`unique`** — Built-in generic test failing if any duplicate non-null value exists.

**`unique_key`** — Incremental config naming the column(s) used as the merge join key. Not a constraint — the warehouse doesn't enforce.

---

## **V**

**`var()`** — Jinja function reading project variables. Set in `dbt_project.yml` under `vars:` or via CLI `--vars '{...}'`.

**`vars`** — Project-level configuration values, overridable on CLI.

**Version** (model versions) — dbt 1.6+ feature for evolving a model's contract without breaking consumers. `ref('m', v=2)`.

**View** — Materialization issuing `CREATE OR REPLACE VIEW`. Cheap to build, expensive to read.

---

## **W**

**Warehouse** — The compute system dbt targets: Snowflake, BigQuery, Databricks, Redshift, Postgres, DuckDB, etc.

**`where:` (test config)** — Limits a test's scope to a subset: `where: "created_at >= '2025-01-01'"`.

---

## **X / Y / Z**

(no terms)

---

## **TERMS NOT TO CONFUSE**

| Term A | Term B | Difference |
|---|---|---|
| dbt Core | dbt Cloud | Open-source CLI vs SaaS layer |
| `ref()` | `source()` | Project model vs declared raw table |
| Generic test | Singular test | Reusable via YAML vs one-off `.sql` |
| Snapshot | Incremental | History vs efficient append |
| `tests:` | `data_tests:` | Old YAML key vs new (1.8+) |
| `dbt run` | `dbt build` | Models only vs models + tests + snapshots + seeds |
| `var()` | `env_var()` | Project var vs OS env var |
| `target` (Jinja) | target dir | Active env vs output folder |
| Materialization | Strategy | View/table/etc. vs merge/append/etc. |
| `unique_key` | Composite key | Single col vs list of cols |
| `dbt deps` | `dbt build` | Install packages vs run project |
| `pre_hook` | `on-run-start` | Per-model vs per-invocation |
| `manifest.json` | `run_results.json` | DAG dump vs last-run results |

---

## **NEXT STEP**

Final step — verify coverage.

Go to [`16_coverage_audit.md`](16_coverage_audit.md).
