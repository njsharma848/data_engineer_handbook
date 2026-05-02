# **08 — Project Structure: staging / intermediate / marts**

> **Goal:** Refactor the toy project into the layered structure every production dbt project uses. Internalize the staging → intermediate → marts pattern, naming conventions, and per-folder configs.

---

## **1. THE MENTAL MODEL**

A real dbt project follows a **3-layer architecture** that mirrors the medallion / bronze-silver-gold pattern:

```
sources (raw)              ──┐
                             │
models/staging/    stg_*    ─┤    1:1 with sources, light cleanup, view
                             │
models/intermediate/  int_* ─┤    optional helpers, joins, aggregations
                             │
models/marts/                │
    core/   dim_/fct_       ─┤    business-grain, table or incremental
    finance/                 │
    marketing/              ─┘
```

The strict rule:

| Layer | Refs from | Refs to |
|---|---|---|
| `staging` | `source()` only | `staging` (rare) |
| `intermediate` | `staging` | `intermediate` |
| `marts` | `staging`, `intermediate`, other `marts` | (anything) |

**No layer skips. No reverse references.** A mart never refs a source directly. A staging model never refs a mart. This isn't bureaucracy — it's what makes the project navigable, refactor-safe, and CI-fast.

---

## **2. WHY IT EXISTS**

Without layering:

- Refactor a column rename in raw → 50 models break.
- A bug in `customer.email` is computed 12 different ways across the project.
- New analysts don't know "where do I add this metric?" → they create yet another flat model.
- Lineage graph is a hairball.

With layering:

- Source schema change → fix one staging model. Everything downstream uses the cleaned column.
- Aggregations are defined once in `intermediate/`, reused.
- Marts are the only thing stakeholders see. Their stability is decoupled from source churn.
- Lineage graph reads top-to-bottom: raw → staged → intermediate → marts → exposures.

This pattern was codified by dbt Labs in the *"How we structure our dbt projects"* guide. It's the single most important convention to learn.

---

## **3. THE THREE LAYERS — WHAT GOES WHERE**

### **3.1 Staging — `models/staging/`**

**One model per source table.** That's the rule. If `shop_raw.customers` exists, then `stg_shop__customers.sql` exists. No more, no less.

**Allowed transformations:**
- Renames (`id` → `customer_id`).
- Type casts.
- Light string cleaning (`lower(trim(email))`).
- Removing obviously-bad rows (`where deleted_at is null`).
- Computing simple derived columns (`coalesce(name, 'Unknown')`).

**Forbidden in staging:**
- Joins.
- Aggregations.
- Business logic.
- Window functions.

**Materialization:** `view` by default. Cheap to rebuild, always fresh.

```sql
-- models/staging/shop/stg_shop__customers.sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('shop_raw', 'customers') }}
),

renamed as (
    select
        id::int                     as customer_id,
        lower(trim(email))          as email,
        initcap(trim(full_name))    as name,
        signup_date::date           as signup_date,
        created_at::timestamp       as created_at,
        updated_at::timestamp       as updated_at
    from source
)

select * from renamed
```

The `source → renamed → select *` CTE chain is canonical staging boilerplate.

### **3.2 Intermediate — `models/intermediate/`**

Optional. Skip this layer for projects under ~10 models. Use it when:

- A complex aggregation feeds 3+ marts. DRY it here.
- A multi-step transformation needs an obvious intermediate name (`int_orders_pivoted`).
- A join is reused across marts.

**Naming:** `int_<entity>_<verb>.sql` — `int_orders_pivoted`, `int_payments_unioned`, `int_customers_with_metadata`.

**Materialization:** `ephemeral` or `view`. Rarely `table` — intermediate models shouldn't be heavy.

```sql
-- models/intermediate/int_customer_payment_methods.sql
{{ config(materialized='ephemeral') }}

select
    customer_id,
    string_agg(distinct payment_method, ', ') as payment_methods
from {{ ref('stg_shop__payments') }}
group by customer_id
```

### **3.3 Marts — `models/marts/`**

**Where business logic lives.** Subdivided by business domain, not by node type.

```
models/marts/
├── core/                    ← shared dimensions/facts
│   ├── dim_customers.sql
│   ├── dim_products.sql
│   └── fct_orders.sql
├── finance/                 ← finance-team-specific marts
│   └── revenue_recognition.sql
└── marketing/               ← marketing-team-specific marts
    └── customer_ltv.sql
```

**Naming:** `dim_<entity>` for dimensions, `fct_<entity>` for facts. `dim_customers`, `fct_orders`. Older codebases use `<entity>_dim` / `<entity>_fact` — equivalent but less common in 2026.

**Materialization:** `table` for dims and small facts; `incremental` for big facts.

```sql
-- models/marts/core/dim_customers.sql
{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_shop__customers') }}
),

orders_summary as (
    select * from {{ ref('int_customer_orders_summary') }}
),

final as (
    select
        c.customer_id,
        c.email,
        c.name,
        c.signup_date,
        coalesce(o.orders_count, 0)        as orders_count,
        coalesce(o.lifetime_value, 0)      as lifetime_value,
        o.first_order_date,
        o.last_order_date
    from customers c
    left join orders_summary o using (customer_id)
)

select * from final
```

---

## **4. CONFIGURE PER-LAYER IN `dbt_project.yml`**

Set defaults once, override per model only when necessary.

```yaml
models:
  shop_dbt:
    staging:
      +materialized: view
      +schema: staging          # writes to <target_schema>_staging
      +tags: ['staging']
    intermediate:
      +materialized: ephemeral
      +schema: intermediate
      +tags: ['intermediate']
    marts:
      +materialized: table
      +schema: marts
      +tags: ['marts']
      core:
        +tags: ['core']
      finance:
        +tags: ['finance']
        +schema: marts_finance
      marketing:
        +tags: ['marketing']
        +schema: marts_marketing
```

Now any model under `models/staging/` is a view tagged `staging` in the `_staging` schema. Models can override individually.

---

## **5. NAMING CONVENTIONS — A CHEAT SHEET**

| Pattern | Layer | Example |
|---|---|---|
| `stg_<source>__<table>` | staging | `stg_shop__customers`, `stg_stripe__charges` |
| `int_<entity>_<verb>` | intermediate | `int_orders_pivoted`, `int_payments_unioned` |
| `dim_<entity>` | marts | `dim_customers`, `dim_products` |
| `fct_<entity>` | marts | `fct_orders`, `fct_page_views` |
| `mart_<area>_<metric>` | marts | `mart_marketing_attribution` |

The double-underscore in `stg_<source>__<table>` lets you parse the source from the name visually.

**Column conventions:**

| Pattern | Use |
|---|---|
| `<entity>_id` | Primary keys (`customer_id`, `order_id`) |
| `is_<thing>` | Boolean flags (`is_active`, `is_terminal`) |
| `<event>_at` | Timestamp (`created_at`, `paid_at`) |
| `<event>_date` | Date (`order_date`, `signup_date`) |
| `count_<thing>` or `<thing>_count` | Counts |
| `_pct` suffix | Percentages |

Pick one set of conventions and document them. Mixing snake_case and camelCase across layers ruins the project.

---

## **6. THE FILE TREE OF A REAL PROJECT**

```
shop_dbt/
├── dbt_project.yml
├── packages.yml
├── profiles.yml             (in ~/.dbt/, not committed)
│
├── models/
│   ├── staging/
│   │   ├── shop/
│   │   │   ├── _shop__sources.yml
│   │   │   ├── _shop__models.yml
│   │   │   ├── stg_shop__customers.sql
│   │   │   ├── stg_shop__orders.sql
│   │   │   └── stg_shop__products.sql
│   │   └── stripe/
│   │       ├── _stripe__sources.yml
│   │       ├── _stripe__models.yml
│   │       └── stg_stripe__charges.sql
│   │
│   ├── intermediate/
│   │   ├── _int__models.yml
│   │   ├── int_orders_pivoted.sql
│   │   └── int_customer_orders_summary.sql
│   │
│   ├── marts/
│   │   ├── core/
│   │   │   ├── _core__models.yml
│   │   │   ├── dim_customers.sql
│   │   │   ├── dim_products.sql
│   │   │   └── fct_orders.sql
│   │   ├── finance/
│   │   │   ├── _finance__models.yml
│   │   │   └── revenue_recognition.sql
│   │   └── marketing/
│   │       ├── _marketing__models.yml
│   │       └── customer_ltv.sql
│   │
│   ├── exposures/
│   │   └── _exposures.yml
│   │
│   └── docs/
│       └── _docs.md
│
├── macros/
│   ├── audit_columns.sql
│   ├── cents_to_dollars.sql
│   └── safe_divide.sql
│
├── seeds/
│   ├── country_codes.csv
│   └── order_status_labels.csv
│
├── snapshots/
│   ├── customer_tiers_snap.sql
│   └── product_prices_snap.sql
│
├── tests/
│   ├── generic/
│   │   └── valid_email.sql
│   └── orders_match_payments.sql       ← singular test
│
├── analyses/
│   └── one_off_revenue_query.sql
│
└── target/                             ← generated, gitignored
    ├── compiled/
    ├── run/
    ├── manifest.json
    └── run_results.json
```

YAML files start with `_` so they sort to the top in editors. The `<source>__` double-underscore visibly separates source from table name.

---

## **7. THE DAG SHAPE OF A WELL-STRUCTURED PROJECT**

```
shop_raw.customers ─┐                                    ┌── dim_customers ──┐
                    ├── stg_shop__customers ─┐           ├──                ─┤
shop_raw.orders ────┴── stg_shop__orders ────┼─ int_orders_pivoted ──── fct_orders
                                             │                         │    │
                    stg_stripe__charges ─────┘                         │    │
                                                                       │    │
                                                            mart_finance__revenue
                                                                       │    │
                                                            (exposures)│    │
                                                                       └────┘
```

Every horizontal layer is one of: source, staging, intermediate, marts. Cross-layer arrows always go top-to-bottom.

---

## **8. BUILD ALONG — REFACTOR THE TOY PROJECT**

We'll restructure the project from `03_sources_and_ref.md`.

### **Step 1.** Move existing models:

```bash
mkdir -p models/staging/shop models/intermediate models/marts/core
mv models/staging/stg_customers.sql models/staging/shop/stg_shop__customers.sql
mv models/staging/stg_orders.sql    models/staging/shop/stg_shop__orders.sql
mv models/marts/fct_customer_orders.sql models/marts/core/fct_customer_orders.sql
```

### **Step 2.** Update refs in the marts model:

```sql
-- models/marts/core/fct_customer_orders.sql
{{ config(materialized='table') }}

select
    c.customer_id,
    c.name,
    count(o.order_id)        as orders_count,
    sum(o.amount)            as lifetime_value,
    min(o.order_date)        as first_order_date,
    max(o.order_date)        as last_order_date
from {{ ref('stg_shop__customers') }} c
left join {{ ref('stg_shop__orders') }} o using (customer_id)
group by 1, 2
```

### **Step 3.** Add an intermediate. Create `models/intermediate/int_customer_orders_summary.sql`:

```sql
{{ config(materialized='ephemeral') }}

select
    customer_id,
    count(order_id) as orders_count,
    sum(amount)     as lifetime_value,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from {{ ref('stg_shop__orders') }}
group by 1
```

### **Step 4.** Refactor `fct_customer_orders.sql` to use the intermediate:

```sql
{{ config(materialized='table') }}

select
    c.customer_id,
    c.name,
    coalesce(s.orders_count, 0)        as orders_count,
    coalesce(s.lifetime_value, 0)      as lifetime_value,
    s.first_order_date,
    s.last_order_date
from {{ ref('stg_shop__customers') }} c
left join {{ ref('int_customer_orders_summary') }} s using (customer_id)
```

### **Step 5.** Update `dbt_project.yml`:

```yaml
models:
  shop_dbt:
    staging:
      +materialized: view
      +tags: ['staging']
    intermediate:
      +materialized: ephemeral
      +tags: ['intermediate']
    marts:
      +materialized: table
      +tags: ['marts']
```

### **Step 6.** Move sources YAML to live next to staging:

```bash
mv models/_sources.yml models/staging/shop/_shop__sources.yml
```

### **Step 7.** Run the refactored project:

```bash
dbt run
```

```
Found 3 models, 0 data tests, 1 source, 475 macros

1 of 3 START sql view  model main.stg_shop__customers ........ [OK]
2 of 3 START sql view  model main.stg_shop__orders ........... [OK]
3 of 3 START sql table model main.fct_customer_orders ........ [OK]

Done. PASS=3 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=3
```

Note: `int_customer_orders_summary` is ephemeral — it doesn't appear in the run output as a separate node. It got inlined as a CTE in `fct_customer_orders`.

### **Step 8.** Use tag-based selection (a Week-3 CLI superpower):

```bash
dbt run --select tag:staging
dbt run --select tag:marts
dbt run --select staging+        # staging and everything downstream
```

---

## **9. WHEN TO BREAK THE RULES**

The pattern is opinionated. Some honest exceptions:

- **Tiny project (<5 models)** — a flat structure is fine. Don't pre-build a dozen empty folders.
- **One source, one fact, one dim** — staging + marts. No intermediate needed.
- **Reverse-ETL output models** — they live at the marts layer but are flagged `tags: ['reverse_etl']` because their consumer is downstream of dbt.
- **Snapshot of a mart** — rare, but if you want history of a derived table, the snapshot lives in `snapshots/` and is reffed downstream from marts. The "no reverse refs" rule still applies.

The point of the layering isn't ceremony — it's stability under change. Pick the lightest structure that supports that.

---

## **10. REAL-WORLD USE CASES**

- **Salesforce + Stripe + Postgres ingestion** — three folders under `staging/` mirror three sources. Each gets its own `_sources.yml`, `_models.yml`, and a clean view per table.
- **Multi-domain marts** — `core/` for shared dims (`dim_customers`, `dim_products`); `finance/` for revenue recognition; `marketing/` for attribution; `product/` for funnels. Each domain owned by a team.
- **Schema-per-team** — `+schema: finance` puts finance marts in a separate warehouse schema so finance queries can be permissioned separately.
- **Service-level isolation** — different schedules per layer: staging hourly, intermediate every 4 hours, marts nightly. Layered structure makes scheduling tractable.

---

## **11. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Strict layering: source → stg → int → mart.** Skips break refactors.
- **One staging model per source table.** Never two; never zero.
- **Stage everything before joining.** Joins live in intermediate/marts only.
- **Per-folder schema config.** Put staging in `_staging`, marts in `_marts`.
- **Per-folder tags.** Selectors get cheap.
- **Sources YAML lives next to the staging models that consume them.**

### **Anti-patterns**

- **Flat `models/` with 100 files.** Unnavigable.
- **Reverse refs** — staging that joins to a mart for "convenience." You've made it impossible to refactor anything downstream.
- **Skipping staging** — marts that ref `source()` directly. The day the source schema changes, every mart breaks.
- **Multiple staging models for the same source table** — pick the right one and refactor others to it.
- **Naming inconsistency** — `customer_dim` next to `dim_orders` next to `customers_clean`. Pick one convention.
- **Per-model materialization configs** copy-pasted in 50 files. Use folder-level defaults.

---

## **12. INTERVIEW QUESTIONS**

### **Q1. [Foundational] Walk me through the layered structure of a dbt project.**

**Model answer:** Three layers. Staging (`models/staging/`) — one model per source table, light cleanup, named `stg_<source>__<table>`, materialized as views. Intermediate (`models/intermediate/`) — optional helpers, named `int_<entity>_<verb>`, ephemeral or view. Marts (`models/marts/`) — business-grain tables and facts, subdivided by domain (`core/`, `finance/`, `marketing/`), named `dim_<entity>` or `fct_<entity>`, materialized as tables or incrementals. Refs flow top-to-bottom only — staging refs sources, intermediate refs staging, marts ref any of them. Source-direct refs from marts are forbidden.

### **Q2. [Foundational] Why have a staging layer at all?**

**Model answer:** Staging is the project's interface to upstream change. When raw schemas change — column renamed, type changed, source moved — you fix the staging model and everything downstream stays the same because it consumed the cleaned, renamed view. Without staging, every mart hardcodes raw column names and a column rename breaks 30 models. Staging also enforces a "rename and clean once" discipline so the project's cleaned column names are stable across the rest of the project.

### **Q3. [Intermediate] When do you create an intermediate model?**

**Model answer:** When the same join or aggregation is reused across multiple marts, or when a mart's logic gets too long to read in one file. The trigger is DRY violation or readability. Skip intermediates entirely on small projects (<10 models) — they add overhead. Materialize intermediates as `ephemeral` (inlined) or `view`; never as `table` because they're internal helpers, not the project's contract with consumers.

### **Q4. [Intermediate] How do you set per-folder defaults for materialization, schema, and tags?**

**Model answer:** In `dbt_project.yml` under `models:`, with the `+` prefix marking configs (vs sub-folder names):

```yaml
models:
  shop_dbt:
    staging:
      +materialized: view
      +schema: staging
      +tags: ['staging']
    marts:
      +materialized: table
      +schema: marts
      core:
        +tags: ['core']
```

Configs cascade — `models/marts/core/dim_x.sql` inherits the marts defaults plus the core tag. Individual models can override with `{{ config(...) }}`.

### **Q5. [Advanced] Your project has 200 models and a stakeholder asks "what depends on `dim_customers`?" — how do you answer in seconds?**

**Model answer:** Either `dbt list --select dim_customers+` to print the downstream node names, or open `dbt docs serve` and click the model's downstream button. The graph operator `dim_customers+` selects the model and everything downstream (transitively) — that's the lineage answer. For column-level "which fields depend on `dim_customers.email`", stock dbt-core can't tell you — use dbt Cloud column-level lineage, `dbt-osmosis`, or a third-party catalog (DataHub, Atlan).

### **Q6. [Advanced] What's the trade-off between many small marts and a few large ones?**

**Model answer:** Many small marts: easy to test in isolation, fast incremental builds, clear ownership per domain — but fragmentation, more cross-mart joins in BI, harder to maintain consistency of common dimensions. Few large marts: simpler joins for analysts, fewer files, central authority on definitions — but slower full builds, harder to assign ownership, harder to permission. The middle ground most projects land on: shared `core/` (10–20 dims and facts every domain uses) plus thin domain-specific marts that compose them. Optimize for "where would a new analyst look first?"

---

## **13. GOTCHAS**

- **Schema config has a default prefix.** `+schema: marts` produces `<target_schema>_marts`, not `marts`. Override `generate_schema_name` macro to drop the prefix in prod.
- **`source()` from a non-staging layer** parses fine but breaks the layering convention. dbt won't error; CI custom checks must.
- **Folder names are not magical.** `models/staging/` works because `dbt_project.yml` configures it that way. Renaming the folder requires updating the config.
- **Many YAMLs, one model.** A model can have descriptions split across multiple YAML files in older versions; modern dbt errors. Keep one YAML per model definition.
- **Tags are additive, not exclusive.** A model in `models/marts/core/` tagged `core` AND tagged `marts` (cascading from parent folder) — both apply.
- **Per-folder tests** can be defined under `data_tests:` in `dbt_project.yml`. Useful for "every model under this folder must have `unique` on its PK" — but verbose; many shops skip.

---

## **NEXT STEP**

You can structure a project. Now master the most important materialization for production: `incremental`.

Go to [`09_incremental_deep_dive.md`](09_incremental_deep_dive.md).
