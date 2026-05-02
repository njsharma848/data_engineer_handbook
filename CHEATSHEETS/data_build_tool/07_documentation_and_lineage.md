# **07 — Documentation and Lineage**

> **Goal:** Make your dbt project self-documenting. Use doc blocks, YAML descriptions, and `dbt docs serve` to produce a browseable docs site with full lineage. The infographic's "Auto Documentation" pillar lives here.

---

## **1. THE MENTAL MODEL**

Documentation in dbt is **a side effect of writing the project well.** Every YAML description, every `unique` test, every `ref()` call contributes to a generated docs site that:

- Lists every model, source, snapshot, seed.
- Shows column-level descriptions and tests.
- Renders a clickable DAG (lineage graph).
- Lets you click a column to see the SQL that defined it (column-level lineage in newer versions).

```
YAML descriptions + ref() / source() calls
   │
   │ dbt docs generate
   ▼
target/manifest.json + target/catalog.json
   │
   │ dbt docs serve
   ▼
http://localhost:8080  ← browseable site
```

The docs site is **always current** because it's regenerated from the code. No drift. That's the "Auto Documentation" promise.

---

## **2. WHY IT EXISTS**

Pre-dbt docs:

- Confluence pages that go stale within a sprint.
- README.md updates that lag the code.
- "Tribal knowledge" living in a senior engineer's head — extinguished when they leave.
- Metric definitions in a spreadsheet that nobody trusts.

dbt's docs generation is good enough that most teams stop maintaining a separate Confluence — the dbt site is the source of truth. Combined with column-level lineage (dbt 1.6+ via `dbt-osmosis` or paid tools), you can answer "what feeds revenue?" in 5 clicks.

---

## **3. DESCRIBING MODELS, COLUMNS, AND SOURCES IN YAML**

Every node in the DAG can carry a `description:` field. dbt scrapes them at parse and renders them in the docs site.

`models/_models.yml`:

```yaml
version: 2

models:
  - name: stg_customers
    description: |
      One row per customer, cleaned from raw OLTP replica.
      Keys: `customer_id`. PII columns lowercased + trimmed.
    columns:
      - name: customer_id
        description: "Primary key, sourced from `raw.customers.id`"
        data_tests: [unique, not_null]
      - name: email
        description: "Lowercased email; null only if signup pre-2024 (legacy import)"
      - name: signup_date
        description: "Date of account creation. Earliest value: 2020-01-15."

  - name: fct_customer_orders
    description: "Customer × orders aggregate. Grain: one row per customer."
    columns:
      - name: customer_id
        description: '{{ doc("customer_id") }}'   # see doc blocks below
      - name: lifetime_value
        description: "Sum of `amount` across all `stg_orders`. NULL when no orders."
```

A few patterns:

- Use **multi-line YAML** (`|`) for descriptions that span multiple lines.
- Wrap **column names in backticks** so they render as code in the docs site.
- Markdown inside descriptions works.
- Descriptions appear in the model page, the column listing, and tooltips on the lineage graph.

---

## **4. DOC BLOCKS — REUSE LONG DESCRIPTIONS**

If the same definition appears in 5 models (e.g., what `customer_id` means), use a **doc block**.

`models/docs/_docs.md`:

```markdown
{% docs customer_id %}
The unique customer identifier sourced from the OLTP `customers.id` column.

- Type: `bigint`
- Globally unique
- Stable across renames and email changes
- Created at signup; never reused after deletion
{% enddocs %}

{% docs lifetime_value %}
Sum of `amount` across all completed orders for the customer.

- Calculated in `fct_customer_orders.sql`
- Updated nightly
- Currency: USD only (multi-currency on roadmap Q3)
- NULL if customer has zero orders
{% enddocs %}
```

Reference from YAML:

```yaml
columns:
  - name: customer_id
    description: '{{ doc("customer_id") }}'
  - name: lifetime_value
    description: '{{ doc("lifetime_value") }}'
```

`{{ doc("customer_id") }}` is a Jinja call to the `doc()` macro, which resolves to the named block. Update once → propagates to every consumer.

---

## **5. GENERATE AND SERVE THE DOCS**

```bash
dbt docs generate
```

dbt parses the project (yes, again — independent from `dbt run`), introspects the warehouse for column types, and writes:

- `target/manifest.json` — the project graph.
- `target/catalog.json` — column types and table sizes from the warehouse.
- `target/index.html` — the static site bundle.

```bash
dbt docs serve
```

Starts a local web server (default `:8080`) hosting `target/index.html`. Browser → DAG, model pages, column listings, source freshness.

```
Serving docs at 0.0.0.0:8080
To access from your browser, navigate to: http://0.0.0.0:8080
```

**Note:** `dbt docs serve` is for local dev. In production you deploy `target/` to a static host (S3 + CloudFront, GitHub Pages, dbt Cloud's hosted docs).

---

## **6. THE LINEAGE GRAPH (DAG)**

The docs site renders the DAG interactively. Nodes are color-coded by type (model, source, seed, snapshot, exposure). Edges are `ref()` and `source()` dependencies.

```
[shop_raw.customers] ──→ [stg_customers] ──→ [dim_customers] ──→ [fct_customer_orders]
[shop_raw.orders]    ──→ [stg_orders]                       ↗
                                                    [seed: country_codes]
```

Useful interactions:

- Click a node → see model page (description, columns, code, tests).
- Right-click → "Show dependencies upstream/downstream."
- Filter by tag, by folder, by node type.
- Use the **graph operator search** at the top: `+stg_customers+` shows that model and all dependencies in both directions.

This is what "lineage tracking" in the infographic means — `ref()` makes lineage automatic.

---

## **7. EXPOSURES — DOCUMENTING DOWNSTREAM CONSUMERS**

dbt's DAG ends at marts. But your data isn't consumed by dbt — it's consumed by Tableau, Looker, an ML pipeline. **Exposures** add nodes to the DAG that represent these downstream consumers.

`models/exposures/_exposures.yml`:

```yaml
version: 2

exposures:
  - name: weekly_revenue_dashboard
    type: dashboard            # dashboard, notebook, application, ml
    maturity: high
    url: https://looker.shop.com/dashboards/42
    description: "Weekly revenue by region — board-level KPI"
    depends_on:
      - ref('fct_customer_orders')
      - ref('dim_customers')
    owner:
      name: Data Team
      email: data@shop.com
```

Now your DAG ends with the dashboard, not the mart. Anyone changing `fct_customer_orders` sees that the board's dashboard depends on it.

---

## **8. THE BUILD ALONG**

### **Step 1.** Add descriptions to your models. Edit `models/_models.yml`:

```yaml
version: 2

models:
  - name: stg_customers
    description: "Cleaned customers — one row per customer."
    columns:
      - name: customer_id
        description: "Primary key"
        data_tests: [unique, not_null]
      - name: email
        description: "Lowercased; can be NULL for legacy rows"

  - name: stg_orders
    description: "Cleaned orders — one row per order."
    columns:
      - name: order_id
        description: "Primary key"
        data_tests: [unique, not_null]
      - name: customer_id
        description: "Foreign key to stg_customers"

  - name: fct_customer_orders
    description: "Customer × order aggregates. Grain: customer."
    columns:
      - name: customer_id
        description: "Primary key. Foreign key to stg_customers."
        data_tests: [unique, not_null]
      - name: lifetime_value
        description: "Sum of amount across all orders. 0 if no orders."
      - name: orders_count
        description: "Count of orders."
```

### **Step 2.** Add a doc block. Create `models/docs/_docs.md`:

```markdown
{% docs ecommerce_grain %}
The e-commerce dimensional model uses these grains:

- **stg_customers**: 1 row per customer
- **stg_orders**: 1 row per order
- **fct_customer_orders**: 1 row per customer (orders aggregated)
- **fct_orders**: 1 row per order (planned)

When in doubt, run `select count(*), count(distinct <key>) from <model>`.
{% enddocs %}
```

### **Step 3.** Add an exposure `models/exposures/_exposures.yml`:

```yaml
version: 2

exposures:
  - name: customer_ltv_dashboard
    type: dashboard
    maturity: medium
    description: "Customer lifetime value dashboard for marketing"
    depends_on:
      - ref('fct_customer_orders')
    owner:
      name: Marketing Analytics
      email: marketing-data@shop.com
```

### **Step 4.** Generate and serve:

```bash
dbt docs generate
dbt docs serve --port 8080
```

Visit `http://localhost:8080`. You should see:

- A DAG with sources, staging models, marts, and the new exposure node.
- Model pages with your descriptions.
- Column tests showing as little checkmark badges.
- The `customer_ltv_dashboard` exposure linked downstream of `fct_customer_orders`.

### **Step 5.** Click around. Observe:

- Source pages show the source's database/schema/table and the freshness config (if set).
- Model pages have a "Compiled SQL" tab — the actual rendered SQL.
- The "Lineage Graph" button gives you a focused view of one node's dependencies.

---

## **9. WHAT DBT'S DOCS DON'T DO (HONEST CRITIQUE)**

- **No column-level lineage in stock dbt-core.** You see model-to-model edges, not column-to-column. Tools like `dbt-osmosis`, paid features in dbt Cloud, or third-party catalogs (DataHub, Alation, Atlan) fill this gap.
- **No automatic detection of stale descriptions.** A column was renamed in SQL but the YAML still describes the old name? dbt won't tell you (some shops use `dbt-osmosis` to enforce parity).
- **No metric semantics in stock 1.x.** dbt has Semantic Layer (formerly MetricFlow) for metric definitions — separate from docs descriptions. If you don't use the Semantic Layer, metric definitions live in model SQL, not in docs.
- **Search is basic.** Finding "where is `email_normalized` defined?" is keyword search across descriptions and column names. No fuzzy match, no semantic search.
- **Static site only.** No write-back from the UI, no commenting, no role-based access control. dbt Cloud has more.

---

## **10. PERSIST DOCS TO THE WAREHOUSE**

dbt can write your YAML descriptions as **column comments in the warehouse** (Snowflake, BigQuery, Postgres, Databricks). Set:

```yaml
# dbt_project.yml
models:
  +persist_docs:
    relation: true            # write description as table comment
    columns: true             # write column descriptions as column comments
```

Now `\d+ table_name` in psql or `DESCRIBE TABLE` in Snowflake shows your YAML descriptions inline. BI tools that read warehouse metadata (Looker, ThoughtSpot) inherit them automatically.

---

## **11. REAL-WORLD USE CASES**

- **Onboarding** — a new analyst can navigate the docs site to understand any model in 10 minutes.
- **PR reviews** — reviewers comment on description quality, not just SQL.
- **Audits** — regulators ask "what feeds report X?" → click through lineage.
- **Stakeholder self-serve** — non-engineering teams browse the docs site to understand what data exists.
- **Catalog integrations** — the manifest+catalog can be fed to DataHub, Atlan, Alation, OpenLineage for org-wide cataloging.

---

## **12. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Description on every model.** One line minimum. Be honest — say what the grain is.
- **Description on every column** in marts/dim/fct (the layer stakeholders see). Staging can be lighter.
- **Use doc blocks for definitions used in 3+ places.**
- **`persist_docs: { relation: true, columns: true }`** so warehouse-native tools see the comments.
- **Run `dbt docs generate` in CI** and deploy `target/` to a static host. Stakeholders need a stable URL.
- **Add exposures for every BI dashboard** that consumes a mart. Lineage extends to the consumer.
- **Tag models** for filtering: `tags: ['daily', 'finance']`. Tags surface in docs and CLI selectors.

### **Anti-patterns**

- **Empty descriptions.** Worse than no description — they're lies.
- **Copy-pasted descriptions.** If `customer_id` is described identically in 8 models, it should be a doc block.
- **Long prose blocks** that recap the SQL. The SQL is right there. Describe **what** and **why**, not how.
- **Stale descriptions** that don't match the current SQL. Treat docs as code; review them in PRs.
- **Hosting `dbt docs serve` in production.** It's a dev server; deploy the static `target/` files instead.
- **No exposures.** The DAG ends at the mart, lineage is half-finished.

---

## **13. INTERVIEW QUESTIONS**

### **Q1. [Foundational] How does dbt generate documentation?**

**Model answer:** `dbt docs generate` parses the project to produce `target/manifest.json` (the DAG and metadata from YAML) and `target/catalog.json` (column types and table stats from the warehouse). It then writes a static `index.html` bundle that reads those JSON files and renders the docs site. `dbt docs serve` runs a local webserver against the bundle. The site shows every model, source, seed, snapshot, exposure with descriptions, columns, tests, and the lineage graph. In production you'd deploy `target/` to a static host, not run `serve`.

### **Q2. [Foundational] What's a doc block and why use it?**

**Model answer:** A doc block is a named block of markdown defined in a `.md` file under `models/` (or any docs path), wrapped with `{% docs name %}…{% enddocs %}`. You reference it from YAML with `{{ doc('name') }}`. The point is reuse: a single column like `customer_id` might appear in 10 models. Without doc blocks you copy-paste the description 10 times — and they drift. With a doc block, you edit one place and every consumer updates.

### **Q3. [Intermediate] What's an exposure and when would you add one?**

**Model answer:** An exposure is a DAG node representing a downstream consumer of dbt's outputs — a dashboard, a notebook, an ML pipeline, an external app. You declare it in YAML with `depends_on: [ref('mart_model')]`, plus owner, URL, and maturity. Adding exposures extends the lineage graph beyond dbt: a developer changing `fct_orders` can see every dashboard that depends on it. Without exposures the DAG ends at the mart and you have no automated way to assess blast radius downstream.

### **Q4. [Intermediate] What does `persist_docs` do?**

**Model answer:** `persist_docs` writes your YAML descriptions to the warehouse as native column and table comments. With `persist_docs: { relation: true, columns: true }` in `dbt_project.yml`, after `dbt run` you'll see your descriptions when you `DESCRIBE TABLE` or `\d+`. BI tools that read warehouse metadata inherit the descriptions automatically — without `persist_docs`, the descriptions only exist in the dbt docs site.

### **Q5. [Advanced] What can column-level lineage do that model-level lineage can't, and how do you get it?**

**Model answer:** Model-level lineage tells you "this model depends on these models." Column-level lineage tells you "this column is computed from these other columns." That's what enables impact analysis like "what feeds the `revenue` column on the board dashboard?" — model-level only narrows it to the model. Stock dbt-core doesn't provide column-level lineage. You get it from: dbt Cloud (paid feature), `dbt-osmosis` (open source plugin), or third-party catalogs (DataHub, Alation, Atlan, OpenLineage). All of them parse the compiled SQL to derive column-to-column edges. Some teams build it in-house using SQL parsers like SQLGlot.

### **Q6. [Advanced] How would you keep documentation in sync with the SQL when columns are renamed or added?**

**Model answer:** A few mechanisms:
1. **`dbt-osmosis`** — open-source CLI that propagates column descriptions through the DAG and inserts placeholder YAML for new columns. Runs in CI.
2. **`dbt-checkpoint`** — pre-commit hooks that fail if a model has columns missing from YAML.
3. **Custom CI step** — diff `manifest.json` columns against YAML; fail if mismatch.
4. **Convention** — every PR adding a column also updates YAML, enforced by review.

The risk without enforcement is exactly what dbt set out to fix: drift. The cure is automation in CI.

---

## **14. GOTCHAS**

- **`dbt docs generate` requires a successful run first** — it queries the warehouse for column metadata. If the warehouse doesn't yet have the tables, `catalog.json` is incomplete.
- **`dbt docs serve` defaults to port 8080.** Conflicts with Tomcat-style apps; use `--port 9999` to override.
- **Doc blocks must live under a path in `docs-paths` config** (defaults are `models/`, `analyses/`, `macros/`, etc., depending on version). Don't drop them in random places.
- **YAML duplication** — if you define `models:` in both `_a.yml` and `_b.yml` for the same model name, dbt errors with a clean message. Each model is described in exactly one place.
- **Description quoting** — single quotes inside a YAML description without escaping can break the parser. Use double quotes or `|` block style.
- **`persist_docs` doesn't work on every adapter** — most major ones support it (Snowflake, BigQuery, Postgres, Databricks). Check the adapter docs.
- **Tags in `dbt_project.yml`** apply to a folder; tags in a model file apply to that model. Both work; don't conflict them.

---

## **NEXT STEP**

You've finished the Week 2 quality and documentation foundations. Now scale to a production-grade project structure.

Go to [`08_project_structure.md`](08_project_structure.md).
