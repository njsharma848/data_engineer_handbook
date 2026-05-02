# **04 — Tests: Generic, Singular, and Custom**

> **Goal:** Use dbt's testing framework like a software engineer uses unit tests. Cover generic tests, singular tests, custom generic tests, and the two killer testing packages (`dbt_utils`, `dbt_expectations`). Catch bad data before stakeholders do.

---

## **1. THE MENTAL MODEL**

A dbt test is a **SQL query that should return zero rows**. If it returns ≥1 row, the test fails — those rows are the offending records.

That's the entire abstraction. Every test in dbt — the simplest `unique` to the gnarliest custom assertion — compiles to a SELECT. If it returns rows, you've got a problem.

```
A dbt test:
    SELECT bad_records FROM ...   ← compiled SQL
    Expected: 0 rows
    Got: 5 rows  → FAIL
```

This is profoundly different from runtime exceptions in Python tests. dbt tests run **after** the model is built, against the just-materialized data. Failures don't roll back the model — they alert that the model contains bad data.

---

## **2. WHY TESTS EXIST**

Pre-dbt, the standard data-quality story was:

- **No tests.** Data wrong → dashboard wrong → angry exec → 2 AM Slack message → emergency hotfix.
- **Hand-rolled SQL checks** in scattered Jenkins jobs, no central reporting.
- **Great Expectations** (powerful but heavy, separate Python framework).

dbt's pitch: tests live next to the models they test, in the same repo, in the same YAML, run by the same `dbt build` command. Zero context switch. PR adds a test → CI runs it → fails the build before merge.

The four canonical generic tests — `unique`, `not_null`, `accepted_values`, `relationships` — cover ~70% of real-world data quality defects. Add `dbt_utils` and `dbt_expectations` for the long tail.

---

## **3. THE FOUR KINDS OF TESTS IN dbt**

| Type | Defined In | Reusable? | Use Case |
|---|---|---|---|
| **Generic test** | YAML schema files | Yes (across columns/models) | `unique`, `not_null`, `accepted_values`, `relationships` |
| **Singular test** | A single `.sql` file under `tests/` | No (one assertion, one file) | "Total sales today shouldn't be negative" |
| **Custom generic test** | A macro file (`tests/generic/*.sql`) | Yes (you wrote it once, applied many times) | Org-specific patterns like "valid email format" |
| **Package test** | Imported via `packages.yml` | Yes | `dbt_utils.expression_is_true`, `dbt_expectations.expect_column_*` |

---

## **4. GENERIC TESTS — THE FOUR BUILT-INS**

### **4.1 Defining tests in YAML**

Every model can have a sibling `_models.yml` (or any name) listing its columns and tests. Tests sit under each column:

`models/staging/_staging.yml`:

```yaml
version: 2

models:
  - name: stg_customers
    description: "One row per customer, cleaned from raw"
    columns:
      - name: customer_id
        description: "Primary key"
        data_tests:                         # dbt 1.8+ keyword
          - unique
          - not_null
      - name: email
        data_tests:
          - not_null
      - name: signup_date
        data_tests:
          - not_null

  - name: stg_orders
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: status
        data_tests:
          - accepted_values:
              values: ['pending', 'shipped', 'cancelled', 'returned']
      - name: customer_id
        data_tests:
          - relationships:
              to: ref('stg_customers')
              field: customer_id
```

> **Version note:** dbt 1.8+ uses `data_tests:`. dbt ≤1.7 used `tests:`. Both still work for now but `data_tests:` is the future-proof key. Older codebases will use `tests:` — it's not wrong, just legacy.

### **4.2 What each generic test compiles to**

Internally, every generic test is a macro that returns a SELECT.

#### `unique`
```sql
-- Compiled (simplified)
select customer_id from main.stg_customers
where customer_id is not null
group by customer_id
having count(*) > 1
```
Returns the duplicate IDs. Zero rows = pass.

#### `not_null`
```sql
select customer_id from main.stg_customers where customer_id is null
```

#### `accepted_values`
```sql
select status from (select distinct status from main.stg_orders) o
where status not in ('pending', 'shipped', 'cancelled', 'returned')
```

#### `relationships`
```sql
select customer_id from main.stg_orders
where customer_id is not null
  and customer_id not in (select customer_id from main.stg_customers)
```
This is the canonical "foreign key" test — every value in this column must exist in the parent model.

### **4.3 Run them**

```bash
dbt test
# OR
dbt test --select stg_customers
# OR
dbt build      # runs models AND tests in DAG order
```

`dbt build` is preferred in real workflows: it interleaves runs and tests so a failed upstream test stops downstream models from being built (configurable).

---

## **5. SINGULAR TESTS — ONE-OFF ASSERTIONS**

Sometimes the assertion isn't reusable. Drop a `.sql` file under `tests/` (any path):

`tests/no_negative_lifetime_value.sql`:

```sql
-- A test passes when this query returns ZERO rows.
select customer_id, lifetime_value
from {{ ref('fct_customer_orders') }}
where lifetime_value < 0
```

That's the entire test. dbt picks it up at parse, runs it on `dbt test` / `dbt build`, fails if any rows come back.

Singular tests excel at:
- **Cross-model invariants**: "no customer in `dim_customers` is missing from `fct_orders`."
- **Business rules** that don't fit a clean column-level pattern: "sum of orders by status should equal total orders."

Anti-pattern: a singular test that's actually a single-column not-null/unique. That belongs in YAML.

---

## **6. CUSTOM GENERIC TESTS — REUSABLE BUSINESS RULES**

If you find yourself writing the same singular-test pattern 5 times, promote it to a generic test.

### **6.1 Define a custom generic test**

`tests/generic/test_valid_email.sql`:

```sql
{% test valid_email(model, column_name) %}

select {{ column_name }} as bad_value
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not like '%@%.%'

{% endtest %}
```

Line-by-line:

- `{% test valid_email(...) %}` — declares a test macro named `valid_email` that takes two args: the model relation and the column name. dbt passes those automatically when the test is applied.
- `from {{ model }}` — `model` is the relation passed in (e.g. `stg_customers`). At compile this becomes the fully-qualified table name.
- `where ... not like '%@%.%'` — the assertion logic. Returns rows that *fail* the test.
- Returns: rows where the email doesn't have `@` and `.` — bad values. Zero rows = pass.

### **6.2 Apply it like any built-in**

```yaml
models:
  - name: stg_customers
    columns:
      - name: email
        data_tests:
          - not_null
          - valid_email
```

### **6.3 Custom generic with arguments**

```sql
{% test value_within_range(model, column_name, min_value, max_value) %}
select {{ column_name }} from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
```

Use it:

```yaml
columns:
  - name: amount
    data_tests:
      - value_within_range:
          min_value: 0
          max_value: 100000
```

---

## **7. PACKAGE TESTS — `dbt_utils` AND `dbt_expectations`**

You almost never need to write custom tests. The community already did.

### **7.1 Install packages**

`packages.yml` (project root):

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
```

```bash
dbt deps
```

`dbt deps` reads `packages.yml`, fetches the packages, and unpacks them into `dbt_packages/` (gitignored). Their macros and tests are now available in your project.

### **7.2 Killer tests from `dbt_utils`**

```yaml
models:
  - name: fct_orders
    data_tests:
      # Composite uniqueness
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [order_id, customer_id]

      # Arbitrary SQL expression
      - dbt_utils.expression_is_true:
          expression: "amount >= 0"

      # Row count match between two models
      - dbt_utils.equal_rowcount:
          compare_model: ref('fct_orders_legacy')

    columns:
      - name: order_id
        data_tests:
          - dbt_utils.not_empty_string
```

### **7.3 Killer tests from `dbt_expectations`**

`dbt_expectations` ports Great Expectations' assertion library to dbt:

```yaml
columns:
  - name: amount
    data_tests:
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0
          max_value: 1000000
      - dbt_expectations.expect_column_values_to_match_regex:
          regex: "^\\d+\\.\\d{2}$"

  - name: email
    data_tests:
      - dbt_expectations.expect_column_values_to_match_regex:
          regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

Don't write a regex email test by hand. Don't write a "between min and max" test by hand. These are solved problems.

---

## **8. TEST CONFIGURATION — SEVERITY, WHERE, STORE_FAILURES**

### **8.1 `severity: warn`**

A test usually fails the build when it returns ≥1 row. Sometimes you want to be alerted but not blocked:

```yaml
- name: amount
  data_tests:
    - dbt_utils.expression_is_true:
        expression: "amount >= 0"
        config:
          severity: warn      # log a WARN, don't fail the run
```

`severity: error` is the default. Use `warn` for tests that are aspirational — you want visibility, not a paged engineer.

### **8.2 `where:` — scope a test to a subset**

```yaml
- name: customer_id
  data_tests:
    - not_null:
        config:
          where: "order_date >= '2025-01-01'"   # don't fail on legacy backfill
```

### **8.3 `store_failures: true`**

```yaml
- name: customer_id
  data_tests:
    - relationships:
        to: ref('stg_customers')
        field: customer_id
        config:
          store_failures: true
```

When a test fails, dbt persists the failing rows in a `dbt_test__audit` schema. You can `select * from dbt_test__audit.relationships_…` to inspect what went wrong. Invaluable for debugging.

### **8.4 `error_if`/`warn_if` thresholds (dbt 1.5+)**

```yaml
- name: customer_id
  data_tests:
    - not_null:
        config:
          error_if: ">100"      # error only if more than 100 nulls
          warn_if:  ">10"       # warn if more than 10
```

Useful for tolerating known low-quality data.

---

## **9. SOURCE FRESHNESS — A TEST ON RAW DATA**

```yaml
sources:
  - name: shop_raw
    loaded_at_field: _ingested_at
    freshness:
      warn_after: { count: 12, period: hour }
      error_after: { count: 24, period: hour }
    tables:
      - name: orders
```

```bash
dbt source freshness
```

dbt queries `select max(_ingested_at) from raw.orders` and compares to current time. If older than 24h, errors. Output goes to `target/sources.json` for downstream alerting.

---

## **10. BUILD ALONG — ADD TESTS TO YOUR PROJECT**

Continuing the e-commerce project from `03_sources_and_ref.md`.

### **Step 1.** Add `models/_models.yml`:

```yaml
version: 2

models:
  - name: stg_customers
    columns:
      - name: customer_id
        data_tests: [unique, not_null]
      - name: email
        data_tests: [not_null]

  - name: stg_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: status
        data_tests:
          - accepted_values:
              values: ['shipped', 'pending', 'cancelled', 'returned']
      - name: amount
        data_tests:
          - not_null

  - name: fct_customer_orders
    columns:
      - name: customer_id
        data_tests: [unique, not_null]
```

### **Step 2.** Add a singular test `tests/no_negative_lifetime.sql`:

```sql
select customer_id, lifetime_value
from {{ ref('fct_customer_orders') }}
where lifetime_value < 0
```

### **Step 3.** Run `dbt build` (models + tests in DAG order):

```bash
dbt build
```

**Expected output (passes):**

```
Found 3 models, 8 data tests, 1 source, 475 macros

1 of 11 START sql view  model main.stg_customers ............... [OK]
2 of 11 START sql view  model main.stg_orders .................. [OK]
3 of 11 START test unique_stg_customers_customer_id ............ [PASS]
4 of 11 START test not_null_stg_customers_customer_id .......... [PASS]
5 of 11 START test not_null_stg_customers_email ................ [FAIL 1]
   ↑ This fails because Dan has NULL email — see file 01 step 5.2
...
Done. PASS=10 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=11
```

The `not_null` on email fails — that's a real defect. In production you'd either:
1. Fix the upstream data quality issue.
2. Relax the test to `severity: warn`.
3. Filter out nulls in `stg_customers`.

### **Step 4.** Add `dbt_utils` and use a composite uniqueness test.

Create `packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
```

Run:
```bash
dbt deps
```

Update `_models.yml`:
```yaml
- name: fct_customer_orders
  data_tests:
    - dbt_utils.expression_is_true:
        expression: "lifetime_value >= 0"
```

Run again:
```bash
dbt test --select fct_customer_orders
```

---

## **11. INSPECTING FAILURES**

### **Get the compiled test SQL**

```bash
cat target/compiled/shop_dbt/models/_models.yml/not_null_stg_customers_email.sql
```

You see the exact `select email from main.stg_customers where email is null` query. Run it in DuckDB to see the rows. This is how you debug a test failure: read the compiled SQL, run it, look at the results.

### **Use `store_failures` for inspection**

Configure once at the project level:

```yaml
# dbt_project.yml
data_tests:
  +store_failures: true
```

Now after a failed run:

```sql
-- Show the failing rows
SELECT * FROM dbt_test__audit.not_null_stg_customers_email;
```

---

## **12. REAL-WORLD USE CASES**

- **Onboarding a new data source.** Declare it as a source, slap `unique`/`not_null` on the PK, and `dbt source freshness` on the load timestamp. Within an hour of integration, you have basic quality checks.
- **Migration cutover.** `dbt_utils.equal_rowcount` between old and new pipelines — fails the build if they diverge.
- **Regulatory checks.** Singular tests for "no PII in the wrong table," "no orders dated in the future," "amounts conform to currency precision."
- **CI gating.** GitHub Actions runs `dbt build` on PRs. Failed test = blocked merge. (File 11 covers Slim CI.)
- **Stakeholder-facing data contracts.** dbt 1.5+ supports model contracts with column types and constraints. Tests + contracts = enforced schema.

---

## **13. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Test every primary key.** `unique` + `not_null` is mandatory.
- **Test every foreign key.** `relationships` to the parent.
- **Test categorical columns** with `accepted_values`. Catches new enum values silently appearing.
- **Use `dbt_expectations` for everything regex/range/distribution-based.** Don't reinvent.
- **Default to `severity: error`. Demote to `warn` only when proven necessary.**
- **Write singular tests for cross-model invariants** that no column-level test can express.

### **Anti-patterns**

- **No tests.** You're not using dbt — you're using SQLAlchemy with extra steps.
- **Tests on staging only, none on marts.** Marts are what stakeholders see; bugs there are the most expensive.
- **Singular tests where a generic would do.** Maintainability nightmare.
- **`severity: warn` everywhere.** Tests become noise; nobody reads them.
- **Tests on raw sources you don't own.** Test only what's reasonable to enforce — for raw, freshness + maybe PK uniqueness; everything else belongs in staging.
- **`limit 100` in tests "for performance."** Either it's a real test (no limit) or it isn't.

---

## **14. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What are the four built-in generic tests in dbt?**

**Model answer:** `unique`, `not_null`, `accepted_values`, and `relationships`. `unique` checks no duplicate non-null values in a column. `not_null` checks no nulls. `accepted_values` checks every value is in a given list. `relationships` checks every value in this column exists in another model's column — that's the foreign-key test. All four compile to a SELECT that returns failing rows; zero rows = pass.

### **Q2. [Foundational] What's the difference between a generic test and a singular test?**

**Model answer:** A generic test is a parameterized macro you apply via YAML to many columns or models — `unique`, `not_null`, custom ones you define, or imported from packages. A singular test is a one-off `.sql` file under `tests/` containing a SELECT that should return zero rows. Use generic for reusable patterns; singular for one-off cross-model invariants like "sum of A equals sum of B."

### **Q3. [Intermediate] How does `dbt build` handle test failures vs `dbt run` followed by `dbt test`?**

**Model answer:** `dbt build` runs models, tests, snapshots, and seeds in DAG order — so a failed test on `stg_customers` skips downstream `fct_customer_orders` (and its tests) by default. `dbt run` followed by `dbt test` runs all models first regardless, then all tests; downstream models are built even on upstream test failure. `build` is the production-correct command for catching bad data before propagation; `run`+`test` is fine in dev when you want all models materialized for inspection.

### **Q4. [Intermediate] When would you use `severity: warn` instead of `severity: error`?**

**Model answer:** When a test is informational rather than blocking — for example, a tolerance test for known data quality issues during migration, or an aspirational check on a freshly onboarded source. The default is `error`, which fails `dbt build` and (in CI) blocks merges. `warn` produces a WARNING in the logs and counts toward `--warn-error` if you opt in. It's also useful with `error_if: ">N"` thresholds — "warn at any nulls, only error past 100."

### **Q5. [Advanced] How would you implement a custom generic test for "this date column is always within the last 30 days"?**

**Model answer:**

```sql
-- tests/generic/test_recent_dates.sql
{% test recent_dates(model, column_name, days=30) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < current_date - interval '{{ days }}' day
{% endtest %}
```

Apply via YAML:
```yaml
- name: order_date
  data_tests:
    - recent_dates:
        days: 30
```

The macro takes `model`, `column_name`, and an optional `days` parameter (default 30). It compiles to a SELECT for offending rows. The pattern is reusable across any model.

### **Q6. [Advanced] What happens internally when a test fails with `store_failures: true`?**

**Model answer:** dbt creates a schema (typically `<schema>_dbt_test__audit`) and writes the failing rows into a table named after the test. Subsequent failures overwrite the same table. You can query it directly to inspect what failed without re-running the test SQL by hand. The persisted failures are also retrievable from `target/run_results.json`. The trade-off is storage cost — failures persist indefinitely unless cleaned up — and slightly slower test runtime since it does INSERT instead of SELECT only.

### **Q7. [Advanced] How would you test a cross-model invariant like "the sum of `fct_orders.amount` equals the sum of `stg_orders.amount`"?**

**Model answer:** Singular test. Create `tests/orders_amounts_match.sql`:

```sql
with f as (select sum(amount) as total from {{ ref('fct_orders') }}),
     s as (select sum(amount) as total from {{ ref('stg_orders') }})
select f.total, s.total
from f, s
where abs(f.total - s.total) > 0.01     -- floating-point tolerance
```

Returns one row if the invariant fails, zero rows otherwise. The `abs(... ) > 0.01` tolerates penny-level rounding. Singular tests are perfect for invariants that span multiple models because no column-level generic test can express "compare aggregations across two tables."

---

## **15. GOTCHAS**

- **`tests:` vs `data_tests:` in YAML.** dbt 1.8+ standardized on `data_tests:`; older docs and code use `tests:`. Both work today but `data_tests:` is forward-compatible.
- **Test failures don't roll back the model.** The bad data is already materialized. Downstream consumers will see it unless you use `dbt build` (which skips downstream on failure).
- **Generic tests must have unique names per (model, column, test, args).** Two tests on the same column with the same args collide. dbt errors with a clear message.
- **`relationships` test is not a foreign key constraint.** It's a query — the warehouse doesn't enforce it. If you write to the table outside dbt between runs, the test won't catch it.
- **`accepted_values` is exact match.** Whitespace, case differences fail. Normalize values upstream first.
- **Custom generic tests must live under `tests/generic/` (or another path in `test-paths`).** They won't be discovered if dropped in `macros/` even though they're macro-like.
- **Source freshness has no notion of "expected schedule."** It only checks "is the latest row older than N hours." If your source loads weekly, set `error_after` to 8 days.
- **`store_failures` schema can pollute your warehouse** if you don't clean up old test audit tables. Add a `clean_failures` post-hook or scheduled vacuum.

---

## **NEXT STEP**

You can write tests. Now learn how to keep your code DRY — Jinja and macros.

Go to [`05_macros_and_jinja.md`](05_macros_and_jinja.md).
