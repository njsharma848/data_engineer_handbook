# **dbt (data build tool) — Interview Prep & Hands-On Curriculum**

> **SQL, engineered.**
>
> dbt fixes the three perennial pain points of analytics SQL — no version control, no tests, no documentation — by treating SQL transformations as software. This curriculum gets you from "never installed it" to "interview-ready" in 3–4 weeks.

---

## **WHO THIS IS FOR**

You write strong SQL daily — joins, CTEs, window functions are second nature. You've never used dbt. You have a Data Engineer interview in 3–4 weeks where dbt will come up. You learn by doing, not by reading.

This curriculum **assumes SQL fluency** — no `SELECT *` explanations. Every dbt concept is built from first principles, with a runnable DuckDB exercise attached.

---

## **WHAT YOU'LL LEARN**

| Pillar | What dbt Provides | Pre-dbt Pain |
|---|---|---|
| Version control | SQL in Git, peer-reviewed via PR | `query_v3_FINAL_use_this.sql` |
| Testing | Declarative tests on every model | "It's wrong in the dashboard" |
| Documentation | Auto-generated, always current | A Slack message from 2022 |
| Lineage | DAG showing every dependency | Tribal knowledge |
| Modularity | `ref()` + macros + packages | Copy-pasted CTEs |

After this curriculum you can:
- Spin up a dbt project from scratch and connect it to any warehouse.
- Decide between `view` / `table` / `incremental` / `ephemeral` based on cost & freshness.
- Write tests, snapshots, and macros that survive code review.
- Read a `manifest.json` and `run_results.json`.
- Defend dbt design choices — and call out its trade-offs — in an interview.

---

## **TABLE OF CONTENTS**

```
WEEK 1 — Foundations + First Project
  00_start_here.md ............... Modern data stack, where dbt fits, ELT vs ETL
  01_setup_and_first_model.md .... Install dbt-core, init project, DuckDB, first model
  02_models_and_materializations.md  view / table / ephemeral / incremental
  03_sources_and_ref.md .......... ref() and source(), the DAG

WEEK 2 — Quality, Reusability, History
  04_tests.md .................... Generic, singular, custom; dbt_utils, dbt_expectations
  05_macros_and_jinja.md ......... Jinja from scratch, writing macros
  06_snapshots_and_seeds.md ...... SCD Type 2 + when to use seeds
  07_documentation_and_lineage.md  Doc blocks, dbt docs serve

WEEK 3 — Production-Grade
  08_project_structure.md ........ staging / intermediate / marts conventions
  09_incremental_deep_dive.md .... merge / delete+insert / append, unique_key
  10_hooks_vars_configs.md ....... pre/post hooks, vars, profiles.yml
  11_cicd_and_debugging.md ....... Slim CI, state:modified+, run_results.json

WEEK 4 — Interview Prep
  12_why_dbt_wins_2026.md ........ Industry adoption, alternatives, talking points
  13_cheat_sheet.md .............. CLI, project file structure, Jinja quick-ref
  14_interview_questions.md ...... 30 questions ranked by difficulty
  15_glossary.md ................. Every dbt term you need
  16_coverage_audit.md ........... Final audit table — verified coverage
```

The source infographic this curriculum is built around is at [`dbt_learnings.jpg`](dbt_learnings.jpg).

---

## **3-WEEK STUDY PLAN**

The plan assumes ~1 hour/day on weekdays, 2 hours on weekends. Push to 4 weeks if you also need to build a portfolio project alongside.

### **Week 1 — Foundations (5–6 hrs total)**

| Day | File | Goal | Deliverable |
|---|---|---|---|
| Mon | `00_start_here.md` | Place dbt on the modern data stack mental map | Be able to draw "Raw → dbt → Warehouse" without thinking |
| Tue | `01_setup_and_first_model.md` | Install + run first model | `dbt run` succeeds locally on DuckDB |
| Wed | `02_models_and_materializations.md` | Pick the right materialization | A toy model in each materialization mode |
| Thu | `03_sources_and_ref.md` | Wire models with `ref()` and `source()` | Lineage graph with 3+ nodes |
| Fri | Review + recap | Re-derive the DAG by hand | Quiz yourself on Week 1 interview Qs |
| Sat | Catch-up / build portfolio | — | Push to GitHub |

### **Week 2 — Quality, Reusability, History (6–7 hrs total)**

| Day | File | Goal | Deliverable |
|---|---|---|---|
| Mon | `04_tests.md` | Generic + singular tests, packages | Failing test that catches a real bug |
| Tue | `05_macros_and_jinja.md` | Read & write macros | A macro that DRYs up 2+ models |
| Wed | `06_snapshots_and_seeds.md` | SCD Type 2 with snapshots | Snapshot capturing a row update |
| Thu | `07_documentation_and_lineage.md` | `dbt docs serve` | Browseable docs site locally |
| Fri | Review + recap | Add docs + tests to Week 1 models | Green `dbt build` |
| Sat | Catch-up | — | — |

### **Week 3 — Production-Grade (7–8 hrs total)**

| Day | File | Goal | Deliverable |
|---|---|---|---|
| Mon | `08_project_structure.md` | staging / intermediate / marts | Refactor portfolio to layered structure |
| Tue | `09_incremental_deep_dive.md` | `merge` / `delete+insert` / `append` | An incremental model that runs idempotently |
| Wed | `10_hooks_vars_configs.md` | hooks, vars, env-aware profiles | A pre-hook that grants permissions |
| Thu | `11_cicd_and_debugging.md` | Slim CI + `state:modified+` | Mock CI workflow that only runs changed models |
| Fri | Review | Read your own compiled SQL | Explain `manifest.json` |
| Sat | Catch-up | — | — |

### **Week 4 — Interview Polish (variable)**

| Day | File | Goal |
|---|---|---|
| Mon | `12_why_dbt_wins_2026.md` | Strong talking points on adoption + trade-offs |
| Tue | `13_cheat_sheet.md` | Internalize CLI + structure + Jinja |
| Wed | `14_interview_questions.md` | Drill all 30 with model answers |
| Thu | `15_glossary.md` | Sanity-check vocabulary |
| Fri | `16_coverage_audit.md` | Confirm nothing missed |
| Sat | Mock interview | Talk through a system-design with dbt |

---

## **HOW TO USE THIS CURRICULUM**

1. **Don't skip the Build Along sections.** Every concept file ends with a 5–15 minute hands-on exercise against DuckDB. Reading dbt without running it is like reading swimming articles without getting wet.
2. **Use the same project throughout.** Files reuse a small e-commerce domain (orders, customers, products) so concepts compound rather than reset.
3. **Read Mental Model → Why It Exists → How It Works first** in each file. Skip the syntax until you understand the problem dbt is solving.
4. **For interview cramming:** read `13_cheat_sheet.md` + `14_interview_questions.md` + `15_glossary.md` and skim the rest.

---

## **PREREQUISITES**

You need:
- **Python 3.9+** — `python3 --version`
- **pip** — comes with Python
- **Git** — basic familiarity (clone, branch, commit, PR)
- **SQL** — comfortable with CTEs, window functions, joins, GROUP BY
- **A terminal** — bash/zsh

You do **not** need:
- A cloud warehouse account (we use DuckDB locally — free, file-based, fast)
- Prior dbt experience
- Jinja or templating experience

---

## **THE TAGLINES YOU NEED IN AN INTERVIEW**

> **"dbt is SQL + software engineering practices."**
>
> **"Compiles to native warehouse SQL — no engine of its own."**
>
> **"`ref()` is what makes the DAG possible. Take it away and dbt becomes a fancy text templater."**
>
> **"In 2026, 'I know dbt' is what 'I know SQL' was 10 years ago."**

---

## **NEXT STEP**

Open [`00_start_here.md`](00_start_here.md) and start reading.
