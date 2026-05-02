# **00 — Start Here: The Modern Data Stack and Where dbt Fits**

> **Goal of this file:** Before touching syntax, place dbt correctly on your mental map. Why it exists, what it replaces, what it doesn't. By the end you should be able to argue why a team would adopt dbt — and when they shouldn't.

---

## **1. THE MENTAL MODEL**

dbt is a **transformation layer**. It sits between your raw warehouse data and your dashboards/ML/reports, and it's the place where messy raw data becomes clean, business-ready tables.

Think of dbt as:

```
git + SQL + software-engineering rituals → applied to your warehouse
```

It is **not** an ingestion tool (Fivetran, Airbyte do that). It is **not** a query engine (Snowflake, BigQuery, Databricks, DuckDB do that). It is **not** an orchestrator (Airflow, Dagster, Prefect do that — though dbt is often called from one).

dbt's whole pitch fits on a sticker: **"SQL, engineered."**

---

## **2. WHY dbt EXISTS — THE PRE-dbt PAIN**

Before dbt, the analytics SQL workflow looked like this:

| Pain point | What you actually saw |
|---|---|
| **No version control** | `revenue_query_v3_FINAL_use_this.sql` floating in a shared drive |
| **No tests** | Bugs detected by a stakeholder yelling about a wrong number on a dashboard |
| **No docs** | "Ask Priya, she wrote it 3 years ago" — Priya has left |
| **Lineage in tribal memory** | "If I change `dim_users`, what breaks?" — no idea |
| **DDL spaghetti** | A senior engineer manually running `CREATE TABLE` statements in production |
| **Test/dev parity broken** | Schema drift between dev/staging/prod |

The infographic phrases this perfectly:

> **Without dbt:** SQL in random notebooks → version chaos (v1, v2, v3) → issues found in dashboards not pipeline → docs in someone's head.
>
> **With dbt:** SQL in Git versioned → tests catch issues early in CI/CD → lineage shows every dependency → docs auto-generated and current.

dbt is not the only fix for any one of these — Liquibase versions DDL, Great Expectations runs tests, Atlan documents — but dbt **bundles all five into one workflow**, with SQL as the language.

---

## **3. THE MODERN DATA STACK — WHERE dbt FITS**

```
  ┌──────────────┐    ┌──────────────────┐    ┌──────────────────────────┐    ┌─────────────────┐
  │   Sources    │ →  │     Ingestion    │ →  │     Warehouse + dbt      │ →  │  Consumption    │
  ├──────────────┤    ├──────────────────┤    ├──────────────────────────┤    ├─────────────────┤
  │ Postgres     │    │ Fivetran         │    │ Snowflake / BigQuery /   │    │ Looker / Tableau│
  │ Salesforce   │    │ Airbyte          │    │ Databricks / Redshift /  │    │ Hex / Mode      │
  │ Stripe       │    │ Kafka + Kinesis  │    │ DuckDB                   │    │ ML pipelines    │
  │ S3 events    │    │ Custom Python    │    │                          │    │ Reverse ETL     │
  │              │    │                  │    │      ↑                   │    │                 │
  │              │    │                  │    │      └─ dbt models live  │    │                 │
  │              │    │                  │    │         on top of raw    │    │                 │
  └──────────────┘    └──────────────────┘    └──────────────────────────┘    └─────────────────┘
        EL                    L                              T                       BI / ML
```

dbt is the **T** in **ELT**. Raw data lands in the warehouse, then dbt transforms it into business-ready models — all using the warehouse's own compute.

**Why this matters:** dbt has no engine of its own. It compiles your Jinja-templated SQL into the dialect your warehouse speaks (Snowflake SQL, BigQuery SQL, etc.) and asks the warehouse to run it. That single design choice is why dbt scales from a 1-person startup to a 1000-engineer FAANG team — the warehouse already solved compute.

---

## **4. ETL vs ELT — AND WHY ELT WON**

| | **ETL (legacy)** | **ELT (dbt's world)** |
|---|---|---|
| Order | Extract → **Transform** → Load | Extract → Load → **Transform** |
| Where transforms happen | A separate compute layer (Informatica, SSIS, Spark) | Inside the warehouse |
| Why people did it | Warehouses were expensive and slow | Cloud warehouses are cheap and elastic |
| What it cost | Brittle Java/Spark/SSIS jobs | Cheap storage of raw data |
| What it bought | Smaller warehouse footprint | Replayable transforms, full history of raw |

**The 2010s shift:** Snowflake (2014 GA), BigQuery (2010), Redshift (2013) made warehouse compute cheap enough that "load everything raw, transform with SQL later" became the cheaper, more flexible option. dbt was founded in 2016 to be the missing tool layer for that pattern. It exploded in adoption because the timing was right.

> **Interview-grade phrasing:** "dbt rode the wave of cheap warehouse compute. ELT only makes sense when the warehouse can absorb both the storage of raw data and the transformation workload — once that became true, a SQL-first transformation tool was inevitable."

---

## **5. dbt CORE vs dbt CLOUD**

This comes up in every interview.

| | **dbt Core** | **dbt Cloud** |
|---|---|---|
| What it is | The open-source CLI | A SaaS layer on top of Core |
| License | Apache 2.0 | Commercial (free seats + paid tiers) |
| Where it runs | Your laptop, your CI runner, your Airflow worker | dbt Labs' servers |
| Includes | Compiler, runner, tests, docs generator | All of Core + IDE, scheduler, CI integration, semantic layer, monitoring, alerts |
| Cost | $0 | Per-seat + run-based pricing |
| When you'd pick it | You already have orchestration (Airflow), CI (GitHub Actions), and a Git workflow | You don't have those things; want hosted IDE + scheduler |

**A common interview trap:** an interviewer asks "what's dbt?" and is checking whether you know the distinction. Always answer "dbt Core is the open-source transformation framework — dbt Cloud is the hosted product around it." If you only mention Cloud, you sound like you've never installed dbt yourself.

This curriculum uses **dbt Core** throughout. Everything you learn applies to Cloud — Cloud is just a wrapper.

---

## **6. CORE CONCEPTS — A 30-SECOND PREVIEW**

You'll meet each of these in detail later. Skim now.

| Concept | What it is | File |
|---|---|---|
| **Model** | A `.sql` file that becomes a table or view in the warehouse | `02_…_materializations.md` |
| **Source** | A declared reference to raw data already in the warehouse | `03_sources_and_ref.md` |
| **`ref()`** | The function that turns a list of SQL files into a DAG | `03_sources_and_ref.md` |
| **Test** | An assertion on data — declarative (`unique`, `not_null`) or custom | `04_tests.md` |
| **Macro** | A reusable Jinja+SQL function | `05_macros_and_jinja.md` |
| **Snapshot** | A way to capture SCD Type 2 history of a slowly-changing table | `06_snapshots_and_seeds.md` |
| **Seed** | A static CSV checked into the repo, loaded as a table | `06_snapshots_and_seeds.md` |
| **Materialization** | How a model becomes physical: `view` / `table` / `ephemeral` / `incremental` | `02_…_materializations.md` |

---

## **7. WHAT THE PIPELINE LOOKS LIKE WITH dbt**

```
Raw Data  →  dbt Models  →  Tests  →  Lineage  →  Docs  →  Warehouse
   │            │             │          │          │         │
   │            │             │          │          │         └─ tables/views
   │            │             │          │          └─ auto-generated site
   │            │             │          └─ DAG visualization
   │            │             └─ unique, not_null, custom assertions
   │            └─ versioned SQL with ref() and source()
   └─ landed by Fivetran / Airbyte / Kafka / etc.
```

**Tagline:** *Better SQL. Better Data.*

---

## **8. THE 5-STEP "GET STARTED" PATH (and what each step buys you)**

This is the path the infographic distills, and it's exactly what we'll do in `01_setup_and_first_model.md`:

1. **Install dbt-core** — `pip install dbt-core dbt-<adapter>` → you have the CLI.
2. **Connect to a warehouse** — fill in `profiles.yml` → dbt knows where to send compiled SQL.
3. **Convert one SQL query to a dbt model** — paste your existing query into `models/foo.sql` → it becomes versioned, runnable as `dbt run`.
4. **Add a uniqueness test** — declare `unique` on the primary key column → catches dupes before dashboards do.
5. **Run `dbt build`** — runs models + tests + snapshots + seeds in DAG order, in one command.

Five steps from "no dbt" to "tested transformation in version control." That's the entire pitch.

---

## **9. WHEN dbt IS THE WRONG TOOL**

Dual-side honesty matters in interviews. dbt is overkill or wrong-fit when:

- **You don't have a warehouse.** dbt has nothing to compile against. Spark on raw files? Use Spark, or `dbt-spark`/`dbt-databricks` if you must.
- **Your transforms aren't SQL.** Image processing, complex ML feature engineering, graph algorithms → use the right tool, not dbt.
- **Sub-second freshness requirement.** dbt is a batch tool. Streaming → Flink, Materialize, RisingWave (or `dbt-materialize` for streaming SQL).
- **Single-step query, no reuse.** A one-off ad-hoc query doesn't need a Git repo and a DAG. Just write SQL.
- **Procedural logic dominates.** If your transformation is "loop through rows, call API, branch on response" — that's a Python job, not a dbt model.

> **Interview-grade phrasing:** "dbt is the right tool when your transformation logic is mostly SQL, you have a real warehouse, and you want engineering rigor on top of it. Outside that envelope, force-fitting dbt creates pain."

---

## **10. THE COMPETITIVE LANDSCAPE (skim now, deep dive in `12_why_dbt_wins_2026.md`)**

| Tool | Lane | Overlap with dbt |
|---|---|---|
| **SQLMesh** | Versioned warehouse SQL with virtual environments | Direct competitor; better state management, smaller community |
| **Coalesce** | GUI-driven dbt-like for Snowflake | Same idea, different audience (analysts, less code) |
| **Dataform** | Google's dbt-equivalent (acquired into BigQuery) | Same idea, BQ-locked |
| **Apache Airflow** | Orchestration | Complementary — orchestrates dbt jobs |
| **Spark / Databricks** | Distributed compute | dbt sits on top via `dbt-databricks` |
| **Materialize / RisingWave** | Streaming SQL warehouses | dbt has adapters but isn't designed for streaming |

dbt's moat: community size, package ecosystem (`dbt-utils`, `dbt-expectations`, `dbt-audit-helper`), and the cultural shift it sparked ("analytics engineering").

---

## **11. ANTI-PATTERNS TO AVOID FROM DAY 1**

You'll fall into these the first week. Recognizing them early saves rework:

- **Hardcoding table names** instead of `ref()` / `source()` → breaks the DAG.
- **One giant model** that does it all → uncacheable, untestable, unreviewable.
- **Materializing everything as `table`** → expensive and slow. Default to `view`, escalate.
- **No tests** → you're using dbt as a SQL templater, not as engineering.
- **Running `dbt run` in production via cron on a laptop** → use a real orchestrator/scheduler.

---

## **12. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What problem does dbt solve?**

**Model answer:** dbt brings software-engineering rituals to analytics SQL: version control, testing, documentation, and lineage. Pre-dbt, SQL transformations lived in shared drives or notebooks with no tests, no docs, and no way to know what depended on what. dbt makes SQL a first-class citizen in Git, runs declarative tests against the data, auto-generates a docs site with a DAG, and compiles everything down to native warehouse SQL.

### **Q2. [Foundational] What's the difference between dbt Core and dbt Cloud?**

**Model answer:** dbt Core is the open-source CLI — the compiler, runner, test framework, and docs generator. It's free, Apache-2.0 licensed, and runs anywhere you have Python. dbt Cloud is the commercial SaaS on top: hosted IDE, scheduler, CI integration, semantic layer, monitoring. Most large companies adopt Core and run it in their own Airflow/CI; smaller teams or those without DevOps muscle pick Cloud.

### **Q3. [Foundational] Where does dbt sit on the modern data stack?**

**Model answer:** dbt is the **T** in ELT. Ingestion tools (Fivetran, Airbyte, Kafka) load raw data into the warehouse. dbt then transforms it inside the warehouse, using the warehouse's own compute. BI tools and ML consume the resulting models. dbt has no compute engine of its own — it compiles Jinja-SQL into native warehouse SQL.

### **Q4. [Intermediate] Why did ELT replace ETL, and why does dbt only make sense in an ELT world?**

**Model answer:** ELT replaced ETL because cloud warehouses became cheap and elastic enough to absorb both storage of raw data and the transformation workload. In ETL, you couldn't afford to load everything raw — compute was expensive, storage was constrained, and warehouses were slow. Once Snowflake/BigQuery/Redshift made compute and storage cheap and decoupled, "load raw, transform later" became the cheaper and more flexible option. dbt is purpose-built for that pattern: it pushes the transformation down to the warehouse, which means it inherits whatever scale the warehouse can offer. In a pure ETL world, dbt has nothing to push down to.

### **Q5. [Intermediate] When would you NOT pick dbt?**

**Model answer:** When the transformation isn't mostly SQL (image processing, graph algorithms, complex ML feature engineering), when you don't have a real warehouse (Spark on raw files), when you need sub-second streaming freshness (Flink/Materialize fit better), or when the project is a single ad-hoc query that doesn't justify a Git repo and a DAG. dbt is the right tool when your transformation logic is mostly SQL on a real warehouse and you want engineering rigor.

### **Q6. [Advanced] How would you describe dbt's architectural moat compared to alternatives like SQLMesh or Dataform?**

**Model answer:** dbt's moat isn't technical — it's the community and ecosystem. SQLMesh has technically better state management (virtual environments, no-rebuild on docs-only changes). Dataform is closer to BigQuery. But dbt has the package ecosystem (`dbt-utils`, `dbt-expectations`), the talent pool (every Analytics Engineer has used it), and the cultural shift it sparked. Switching costs from dbt are organizational, not technical. In interviews, the honest framing is: "dbt won the platform war because it was first and good enough; alternatives are technically interesting but face network effects."

---

## **13. GOTCHAS**

- **dbt isn't a warehouse.** "Where is my data?" → in the warehouse you configured in `profiles.yml`. dbt only orchestrates; it never stores.
- **dbt doesn't ingest.** If you're surprised your raw data isn't there, your ingestion tool failed — not dbt.
- **dbt is batch.** A `dbt run` is a discrete, scheduled execution. There's no streaming primitive in stock dbt.
- **`dbt run` ≠ `dbt build`.** `run` runs only models. `build` runs models + tests + snapshots + seeds in DAG order. You almost always want `build`.

---

## **NEXT STEP**

Go to [`01_setup_and_first_model.md`](01_setup_and_first_model.md) — install dbt and run your first model.
