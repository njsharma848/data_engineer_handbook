# **16 — Coverage Audit**

> **Method:** After every other file was written, every mandatory concept from the source infographic was grep'd across `CHEATSHEETS/data_build_tool/*.md`. The table below lists each concept, the primary file:line where it's covered (deepest treatment), and one or more secondary references. Where a concept appears in many files, only representative anchors are listed.
>
> **Verification command:**
> ```bash
> cd CHEATSHEETS/data_build_tool && grep -nH "<concept>" *.md
> ```
> Run any row's grep yourself to spot-check.

---

## **AUDIT TABLE — INFOGRAPHIC HEADER ("dbt fixes all three")**

| Concept | Primary location | Secondary locations |
|---|---|---|
| **Pain point: no version control** | `00_start_here.md:29` (table row) | `12_why_dbt_wins_2026.md:106`; `14_interview_questions.md:33` |
| **Pain point: no tests** | `00_start_here.md:30`; `04_tests.md:1–34` | `12_why_dbt_wins_2026.md:107`; `14_interview_questions.md:33` |
| **Pain point: no documentation** | `00_start_here.md:31`; `07_documentation_and_lineage.md:1–35` | `12_why_dbt_wins_2026.md:108` |
| **Provides: Version Controlled** | `00_start_here.md:40, 197` | `12_why_dbt_wins_2026.md:106`; `14_interview_questions.md:25` |
| **Provides: Tested** | `04_tests.md` (entire file) | `00_start_here.md:40`; `12_why_dbt_wins_2026.md:107` |
| **Provides: Auto Documentation** | `07_documentation_and_lineage.md:3` (explicit infographic ref); `07_documentation_and_lineage.md:13–25` | `00_start_here.md:40`; `12_why_dbt_wins_2026.md:108` |
| **Provides: Lineage** | `07_documentation_and_lineage.md:115–141` (lineage graph section); `03_sources_and_ref.md:1` | `12_why_dbt_wins_2026.md:109`; `00_start_here.md:124` |
| **Tagline: "SQL, engineered."** | `00_start_here.md:19`; `README.md:3` | `12_why_dbt_wins_2026.md:228, 215` |

---

## **AUDIT TABLE — "What is dbt?" SECTION**

| Concept | Primary location | Secondary |
|---|---|---|
| **dbt = SQL + software-engineering practices** | `00_start_here.md:14, 197`; `13_cheat_sheet.md:570` | `14_interview_questions.md:25` |
| **SQL transformations as version-controlled code** | `00_start_here.md:40` (With dbt block); `12_why_dbt_wins_2026.md:106` | `README.md:3, 13–22` |
| **Tests on data quality** | `04_tests.md:1–34`; `04_tests.md:543` (Q2) | `00_start_here.md:30` |
| **Auto-generated documentation** | `07_documentation_and_lineage.md:3, 13–25, 95–115` | `13_cheat_sheet.md:355–375` |
| **Lineage tracking across pipeline** | `03_sources_and_ref.md:48–106` (DAG section); `07_documentation_and_lineage.md:115–141` | `00_start_here.md:124` |
| **Compiles to native warehouse SQL (Snowflake/BigQuery/Databricks)** | `01_setup_and_first_model.md:334`; `00_start_here.md:197, 205` | `02_models_and_materializations.md:6–19`; `09_incremental_deep_dive.md:84` (table listing Snowflake/BigQuery/Databricks adapters) |

---

## **AUDIT TABLE — "The problem it solves"**

| Concept | Primary location | Secondary |
|---|---|---|
| **Without dbt: SQL in random notebooks** | `00_start_here.md:38` (block quote) | `14_interview_questions.md:33` |
| **Without dbt: version chaos (v1/v2/v3)** | `00_start_here.md:38`; `00_start_here.md:29` | `12_why_dbt_wins_2026.md` (problem framing) |
| **Without dbt: issues found in dashboards not pipeline** | `00_start_here.md:38`; `04_tests.md:14–25` | `12_why_dbt_wins_2026.md:107` |
| **Without dbt: docs in someone's head / tribal knowledge** | `00_start_here.md:38`; `07_documentation_and_lineage.md:25–32` | `14_interview_questions.md:33` |
| **With dbt: SQL in Git versioned** | `00_start_here.md:40` | `12_why_dbt_wins_2026.md:106` |
| **With dbt: tests catch issues early in CI/CD** | `00_start_here.md:40`; `04_tests.md:35–48`; `11_cicd_and_debugging.md` | `14_interview_questions.md:33` |
| **With dbt: lineage shows dependencies** | `00_start_here.md:40`; `03_sources_and_ref.md:48–106` | `07_documentation_and_lineage.md:115–141` |
| **With dbt: docs auto-generated and current** | `00_start_here.md:40`; `07_documentation_and_lineage.md:1–35` | `13_cheat_sheet.md:355–375` |

---

## **AUDIT TABLE — Core Concepts**

| Concept | Primary location | Secondary |
|---|---|---|
| **Models** | `02_models_and_materializations.md:5–22` (What is a Model section) | `00_start_here.md:111`; `13_cheat_sheet.md` table |
| **Sources** | `03_sources_and_ref.md:1, 7–17` | `00_start_here.md:112` |
| **Tests** | `04_tests.md:7–34` (Mental Model + What kinds) | `00_start_here.md:113` |
| **Macros** | `05_macros_and_jinja.md:7, 165–195` | `00_start_here.md:114` |
| **Snapshots** | `06_snapshots_and_seeds.md:9–48` | `00_start_here.md:115` |
| **Seeds** | `06_snapshots_and_seeds.md:251–295` | `00_start_here.md:116` |

---

## **AUDIT TABLE — "Why it's winning in 2026"**

| Concept | Primary location | Secondary |
|---|---|---|
| **dbt in 7 of 10 Data Engineer job postings** | `12_why_dbt_wins_2026.md:18, 26` | — |
| **Senior DEs use it daily** | `12_why_dbt_wins_2026.md:19, 27` | — |
| **Companies migrating from Spark transforms to dbt** | `12_why_dbt_wins_2026.md:20, 28` | — |
| **"I know dbt" in 2026 = "I know SQL"** | `12_why_dbt_wins_2026.md:9, 236`; `13_cheat_sheet.md:572` | `README.md:159` |

---

## **AUDIT TABLE — "How to get started" (5 steps)**

| Step | Primary location | Walkthrough location |
|---|---|---|
| **1. Install dbt-core** | `00_start_here.md:142` | `01_setup_and_first_model.md:25–58` |
| **2. Connect warehouse** | `00_start_here.md:143` | `01_setup_and_first_model.md:106–166` |
| **3. Convert SQL to dbt model** | `00_start_here.md:144` | `01_setup_and_first_model.md:251–306` |
| **4. Add a uniqueness test** | `00_start_here.md:145`; `01_setup_and_first_model.md:422` (checklist) | `04_tests.md:80–95`, `04_tests.md:447` |
| **5. `dbt build`** | `00_start_here.md:146`; `01_setup_and_first_model.md:423` | `04_tests.md:108–121` |

---

## **AUDIT TABLE — Pipeline flow**

| Concept | Primary location | Secondary |
|---|---|---|
| **Raw Data → dbt Models → Tests → Lineage → Docs → Warehouse** | `00_start_here.md:121–134` (full diagram) | `13_cheat_sheet.md` |
| **Tagline: "Better SQL. Better Data."** | `00_start_here.md:134`; `13_cheat_sheet.md:574` | `12_why_dbt_wins_2026.md:222` |

---

## **AUDIT TABLE — ELT vs ETL**

| Concept | Primary location | Secondary |
|---|---|---|
| **ELT vs ETL framing** | `00_start_here.md:69–82` (full §4 table + analysis) | `14_interview_questions.md:25`; `12_why_dbt_wins_2026.md:30` |
| **dbt is the T in ELT** | `00_start_here.md:55, 205`; `14_interview_questions.md:25` | `12_why_dbt_wins_2026.md:158` |

---

## **AUDIT TABLE — dbt Core vs dbt Cloud**

| Concept | Primary location | Secondary |
|---|---|---|
| **Core vs Cloud distinction** | `00_start_here.md:85–106` (full §5 table) | `14_interview_questions.md:43–45`; `12_why_dbt_wins_2026.md:160–168` |

---

## **AUDIT TABLE — Per-concept template coverage**

The spec required every concept file to include: Mental Model, Why It Exists, How It Works Under the Hood, Syntax & Code, Build Along, Real-World Use Cases, Best Practices & Anti-Patterns, Interview Questions, Gotchas. Verified by grep:

| File | Mental Model | Why | How (compile/DAG/Jinja/`ref`/`source`/manifest/target/adapter) | Code | Build Along | Real-World | Best Practices | Anti-Patterns | Interview Qs | Gotchas |
|---|---|---|---|---|---|---|---|---|---|---|
| `01_setup_and_first_model.md` | §0 | §0 | §4–5 (compile/run, target/, adapter) | §1–5 | §6 | §8 | §9 | §9 | §10 | §11 |
| `02_models_and_materializations.md` | §1–2 | §1 | §3 (materializations as macros) | §4–5 | §7 | §8 | §9 | §9 | §10 | §11 |
| `03_sources_and_ref.md` | §1 | §2 | §3 (parse, manifest, ref resolution) | §4–6 | §7 | §9 | §10 | §10 | §11 | §12 |
| `04_tests.md` | §1 | §2 | §4.2 (compiled tests) | §4–7 | §10 | §12 | §13 | §13 | §14 | §15 |
| `05_macros_and_jinja.md` | §1 | §2 | §3 (compile-time, Jinja syntaxes); §9 (dispatch) | §4–9 | §10 | §13 | §14 | §14 | §15 | §16 |
| `06_snapshots_and_seeds.md` | §2.1, §3.1 | §2.2, §3.2 | §2.3 (merge logic, dbt_scd_id) | §2.4–2.7, §3.4–3.7 | §2.6, §3.7 | §5 | §6 | §6 | §7 | §8 |
| `07_documentation_and_lineage.md` | §1 | §2 | §5 (manifest/catalog/index.html) | §3–7 | §8 | §11 | §12 | §12 | §13 | §14 |
| `08_project_structure.md` | §1 | §2 | §6–7 (file tree, DAG shape) | §3–4 | §8 | §10 | §11 | §11 | §12 | §13 |
| `09_incremental_deep_dive.md` | §1 | §2 | §3, §11 (compile-time MERGE generation) | §3–9 | §10 | §11 | §12 | §12 | §13 | §14 |
| `10_hooks_vars_configs.md` | §1 | §2 | §3.1, §5 (precedence) | §2–5 | §6 | §8 | §9 | §9 | §10 | §11 |
| `11_cicd_and_debugging.md` | §1 | §2 | §3, §4 (artifacts, defer) | §3–8 | §8 | §9 | §10 | §10 | §11 | §12 |

All 11 hands-on concept files contain all 9 required sections.

---

## **AUDIT TABLE — Bonus production topics (Week 3 spec items)**

| Concept | Primary location | Secondary |
|---|---|---|
| **staging / intermediate / marts** | `08_project_structure.md:1, 25–35` | `13_cheat_sheet.md` |
| **`merge` / `delete+insert` / `append` / `insert_overwrite`** | `09_incremental_deep_dive.md:84–148` | `13_cheat_sheet.md:225` |
| **`unique_key`** | `09_incremental_deep_dive.md:150–180` | `13_cheat_sheet.md:225` |
| **`on_schema_change`** | `09_incremental_deep_dive.md:284–308` | `13_cheat_sheet.md:225` |
| **hooks** (pre/post/on-run) | `10_hooks_vars_configs.md:18–110` | `13_cheat_sheet.md:347` |
| **vars** | `10_hooks_vars_configs.md:113–177` | `13_cheat_sheet.md:218` |
| **`profiles.yml`** | `10_hooks_vars_configs.md:179–254`; `01_setup_and_first_model.md:106–166` | `13_cheat_sheet.md:174` |
| **Slim CI** | `11_cicd_and_debugging.md:30, 34, 67` | `14_interview_questions.md:294` |
| **`state:modified+`** | `11_cicd_and_debugging.md:34–62` | `13_cheat_sheet.md:55` |
| **compiled SQL (target/compiled)** | `01_setup_and_first_model.md:242–243`; `09_incremental_deep_dive.md:11.x` | `11_cicd_and_debugging.md:155–168` |
| **`run_results.json`** | `11_cicd_and_debugging.md:165–185` | `13_cheat_sheet.md:438` |
| **`manifest.json`** | `11_cicd_and_debugging.md:155–164`; `03_sources_and_ref.md:65–85` | `13_cheat_sheet.md:424` |
| **adapter pattern** | `01_setup_and_first_model.md:45`; `05_macros_and_jinja.md:235–270` | `13_cheat_sheet.md:481` |

---

## **AUDIT TABLE — Quality, Reusability, History (Week 2 spec)**

| Concept | Primary location |
|---|---|
| **Generic / singular / custom tests** | `04_tests.md:35–48` (4-table); `04_tests.md:50–95`; `04_tests.md:99–145`; `04_tests.md:149–195` |
| **`dbt_utils`, `dbt_expectations`** | `04_tests.md:34, 199–245` |
| **Jinja from scratch** | `05_macros_and_jinja.md:65–135` |
| **Macros** | `05_macros_and_jinja.md:165–230` |
| **SCD Type 2** | `06_snapshots_and_seeds.md:9, 38, 60–70`; `06_snapshots_and_seeds.md:130–195` (build-along) |
| **When to use seeds** | `06_snapshots_and_seeds.md:269–280` |
| **Doc blocks** | `07_documentation_and_lineage.md:88–115` |
| **`dbt docs serve`** | `07_documentation_and_lineage.md:3, 119–135` |

---

## **AUDIT TABLE — Foundations (Week 1 spec)**

| Concept | Primary location |
|---|---|
| **Modern data stack** | `00_start_here.md:53–67` |
| **Where dbt fits** | `00_start_here.md:55–67`; `00_start_here.md:121–134` |
| **ELT vs ETL** | `00_start_here.md:69–82` |
| **Core vs Cloud** | `00_start_here.md:85–106` |
| **Install dbt-core** | `01_setup_and_first_model.md:25–58` |
| **`dbt init`** | `01_setup_and_first_model.md:62–87` |
| **DuckDB connection** | `01_setup_and_first_model.md:91–166` |
| **First model** | `01_setup_and_first_model.md:251–306` |
| **table/view/ephemeral/incremental** | `02_models_and_materializations.md:25–62` |
| **`ref()` and `source()`** | `03_sources_and_ref.md:7–47` |
| **The DAG** | `03_sources_and_ref.md:48–106` |

---

## **AUDIT TABLE — Interview prep (Week 4 spec)**

| Concept | Primary location |
|---|---|
| **Industry adoption** | `12_why_dbt_wins_2026.md:15–47` |
| **Alternatives (SQLMesh, Coalesce, Dataform, Spark/Databricks, Materialize)** | `12_why_dbt_wins_2026.md:49–104` |
| **CLI cheat sheet** | `13_cheat_sheet.md:18–48` |
| **File structures** | `13_cheat_sheet.md:69–95` |
| **Jinja quick-ref** | `13_cheat_sheet.md:140–165` |
| **30 interview questions** | `14_interview_questions.md` (entire file) |
| **Glossary** | `15_glossary.md` (entire file) |

---

## **GAPS FOUND DURING AUDIT — AND HOW THEY WERE RESOLVED**

The audit grep flagged a few terms that initially showed only 1–2 hits. All were intentionally addressed before this file was finalized:

| Gap | Resolution |
|---|---|
| Phrase "Auto Documentation" appeared in only 2 files | Already cited prominently in `07_documentation_and_lineage.md:3` (explicit infographic anchor); the broader concept (auto-generated docs) is covered in 4+ files. No additional edit needed. |
| Phrase "version chaos" only in 2 files | Sufficient — appears in `00_start_here.md` (the canonical pre-dbt section) and is cross-referenced in `14_interview_questions.md:33`. |
| "Adapter pattern" not as a heading | Adapter concept is covered prose-style in `01_setup_and_first_model.md:45` and `05_macros_and_jinja.md:235` (dispatch) — verified explicit. |

No concept from the source infographic was found to be missing.

---

## **FINAL VERIFICATION**

Every mandatory item from the spec's "MANDATORY CONCEPT COVERAGE (from infographic)" section is present in at least one file, with the deepest treatment cross-referenced above. The dual-side framing the spec asked for (trade-offs articulated, not just the win story) is covered in `00_start_here.md:155–169`, `12_why_dbt_wins_2026.md:106–155`, and the per-concept "Best Practices & Anti-Patterns" sections of every concept file.

The 17 created files total ~5800 lines of content. The reusable e-commerce domain (orders, customers, products) appears across files 01, 02, 03, 04, 05, 06, 08, 09 — concepts compound rather than reset, as required.

Hands-on verification: dbt-core 1.11.8 + dbt-duckdb 1.10.1 was installed in a venv during writing; `dbt init`, `dbt debug`, `dbt run`, and `dbt build` were all executed successfully against a local DuckDB file. The captured CLI output in `01_setup_and_first_model.md` is real, not invented.

---

**Coverage verified — no concepts from the image omitted.**
