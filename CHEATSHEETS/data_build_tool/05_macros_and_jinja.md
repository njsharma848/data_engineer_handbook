# **05 — Macros and Jinja**

> **Goal:** Build a working mental model of Jinja from scratch, then write your own dbt macros. By the end you should be able to read any dbt project's macros folder and know what's happening.

---

## **1. THE MENTAL MODEL**

Jinja is a **string-templating language** that runs *before* SQL is sent to the warehouse. Think of it as a preprocessor:

```
your_model.sql (Jinja + SQL)  →  dbt parses & renders Jinja  →  pure SQL  →  warehouse
```

Everything inside `{{ }}`, `{% %}`, or `{# #}` is Jinja. Everything else is plain SQL that gets passed through unchanged.

```jinja
{# This is a comment, removed at compile #}

select
    {% for col in ['name', 'email', 'phone'] %}
    {{ col }} {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('customers') }}
```

After Jinja renders this becomes:

```sql
select
    name ,
    email ,
    phone
from "shop"."main"."customers"
```

That's it. **Jinja is text manipulation that produces SQL.** The warehouse never sees `{% for %}` — by the time the SQL leaves dbt, all Jinja is gone.

A **macro** is a reusable block of Jinja+SQL — the dbt equivalent of a Python function.

---

## **2. WHY JINJA EXISTS IN dbt**

Plain SQL is verbose and not modular:

- You write the same `case when status in (...) then ...` logic in 10 models.
- You want different behavior in dev vs prod (sample 1% of rows in dev, 100% in prod).
- You want a column list to come from a config, not be hardcoded.
- You want to programmatically generate union-all queries across N partitions.

Pure SQL can't do any of this cleanly. Stored procedures can — but they're warehouse-specific, not version-controllable, and clunky.

Jinja gives dbt:
- **Loops** (`{% for %}`).
- **Conditionals** (`{% if %}`).
- **Variables** (`{% set %}`).
- **Functions** (macros, defined with `{% macro %}`).
- **Imports** between files.

All evaluated at compile time. The compiled SQL is plain warehouse SQL.

---

## **3. THE THREE JINJA SYNTAXES**

| Syntax | What it does | Example |
|---|---|---|
| `{{ expression }}` | **Renders** an expression as text. The result is pasted into the output. | `{{ ref('customers') }}` |
| `{% statement %}` | **Executes** a control statement (no text output). | `{% if x > 0 %}…{% endif %}` |
| `{# comment #}` | A comment, removed at compile. | `{# todo: refactor this #}` |

**Key distinction:** `{{ }}` puts text *into* the SQL. `{% %}` controls flow but produces no text.

---

## **4. JINJA FROM SCRATCH — A 5-MINUTE TOUR**

### **4.1 Variables**

```jinja
{% set my_var = 'hello' %}
select '{{ my_var }}' as greeting
```

Compiles to:

```sql
select 'hello' as greeting
```

### **4.2 Conditionals**

```jinja
{% set env = target.name %}    {# 'dev', 'prod', etc. #}

select *
from {{ ref('orders') }}
{% if env == 'dev' %}
  limit 1000
{% endif %}
```

In dev: adds `limit 1000`. In prod: no limit.

### **4.3 Loops**

```jinja
{% set statuses = ['pending', 'shipped', 'cancelled'] %}

select
    order_id,
    {% for s in statuses %}
    case when status = '{{ s }}' then 1 else 0 end as is_{{ s }}
    {%- if not loop.last -%},{%- endif %}
    {% endfor %}
from {{ ref('stg_orders') }}
```

Compiles to:

```sql
select
    order_id,
    case when status = 'pending'   then 1 else 0 end as is_pending,
    case when status = 'shipped'   then 1 else 0 end as is_shipped,
    case when status = 'cancelled' then 1 else 0 end as is_cancelled
from "shop"."main"."stg_orders"
```

Note: `{%- … -%}` (with hyphens) trims whitespace around the statement, controlling formatting.

### **4.4 Filters (the Jinja `|` operator)**

```jinja
{% set name = 'Alice' %}
{{ name | upper }}                {# ALICE #}
{{ ['a','b','c'] | join(', ') }}  {# a, b, c #}
{{ 'hello world' | length }}      {# 11 #}
```

Filters chain: `{{ value | upper | trim }}`.

### **4.5 Comments and whitespace control**

```jinja
{# This won't render #}

{%- set x = 1 %}                {# leading hyphen strips preceding whitespace #}
{% set y = 2 -%}                {# trailing hyphen strips following whitespace #}
```

Whitespace control (`-`) is what separates compiled SQL that's readable from compiled SQL that's a 50-line wall of blank lines.

---

## **5. dbt'S BUILT-IN JINJA — THE THINGS YOU'LL USE DAILY**

dbt extends Jinja with project-aware functions and variables.

### **5.1 Functions**

| Function | Returns | Example |
|---|---|---|
| `ref('model')` | A `Relation` (compiles to fully-qualified name) | `select * from {{ ref('stg_orders') }}` |
| `source('src','table')` | Same, for declared sources | `from {{ source('shop_raw', 'orders') }}` |
| `config(...)` | None — sets compile-time config | `{{ config(materialized='table') }}` |
| `var('my_var')` | Value of a project var | `where order_date >= '{{ var("start_date") }}'` |
| `env_var('NAME')` | OS env var | `password: "{{ env_var('SF_PASSWORD') }}"` |
| `is_incremental()` | True iff incremental run conditions met | `{% if is_incremental() %} where … {% endif %}` |
| `target` | Object: `name`, `schema`, `database`, `type` | `{{ target.schema }}` |
| `this` | Current model's relation | `select max(id) from {{ this }}` |
| `run_started_at` | Run start timestamp | `{{ run_started_at }}` |
| `dbt_version` | Currently running dbt version string | `{{ dbt_version }}` |

### **5.2 Useful built-in macros**

| Macro | What it does |
|---|---|
| `{{ adapter.dispatch('foo')() }}` | Dispatch to warehouse-specific implementation |
| `{{ get_columns_in_relation(relation) }}` | Returns list of column metadata for a table |
| `{{ run_query(sql) }}` | Executes SQL during compile and returns the result |
| `{{ log('msg', info=True) }}` | Print to stdout during run |
| `{{ exceptions.raise_compiler_error('bad input') }}` | Fail the compile cleanly |

`run_query` is powerful: it lets a model dynamically discover columns, partition keys, etc. before generating SQL.

---

## **6. WRITING YOUR FIRST MACRO**

### **6.1 Where macros live**

Put `.sql` files under `macros/`. Each file can contain one or more macros. dbt auto-discovers them — no import needed in models.

`macros/cents_to_dollars.sql`:

```sql
{% macro cents_to_dollars(column_name, decimals=2) %}
    round( ({{ column_name }} / 100.0)::numeric, {{ decimals }} )
{% endmacro %}
```

Line-by-line:

- `{% macro cents_to_dollars(column_name, decimals=2) %}` — declares a macro `cents_to_dollars` with two args. `decimals=2` is a default.
- The body is a snippet of SQL with `{{ column_name }}` and `{{ decimals }}` interpolated.
- `{% endmacro %}` closes the macro.
- This macro **renders text** when called from `{{ … }}`. It does not return a Python value (use `{%- … -%}` and `set` for that).

### **6.2 Use it in a model**

```sql
-- models/marts/fct_orders_dollars.sql
select
    order_id,
    {{ cents_to_dollars('amount_cents') }} as amount_usd
from {{ ref('stg_orders') }}
```

Compiles to:

```sql
select
    order_id,
    round( (amount_cents / 100.0)::numeric, 2 ) as amount_usd
from "shop"."main"."stg_orders"
```

### **6.3 Use it with arguments**

```sql
{{ cents_to_dollars('amount_cents', decimals=4) }}
```

Compiles to:

```sql
round( (amount_cents / 100.0)::numeric, 4 )
```

---

## **7. A REAL-WORLD MACRO — DRY COLUMN LISTS**

A common pattern: 5 models all `select *` from staging then add some columns. The "cleanup" should be DRY.

`macros/star_except.sql`:

```sql
{% macro star_except(relation, except=[]) %}
    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    {%- for c in cols if c.name not in except -%}
        {{ c.name }}{%- if not loop.last -%},{%- endif %}
    {%- endfor -%}
{% endmacro %}
```

What this does:

- `adapter.get_columns_in_relation(relation)` queries the warehouse during compile to introspect the table's columns. Returns a list of column objects.
- The for-loop emits each column name, comma-separated, **excluding** any in the `except` list.

Use it:

```sql
select
    {{ star_except(ref('stg_customers'), except=['email', 'phone']) }}
from {{ ref('stg_customers') }}
```

Compiles to:

```sql
select
    customer_id,name,signup_date
from "shop"."main"."stg_customers"
```

Why this is useful: when staging adds a new column, all consumers automatically pick it up — no copy-paste.

> **Note:** `dbt_utils.star()` exists and is more featureful (supports prefixes, suffixes, casing). Use it instead in real projects. We wrote this from scratch to demonstrate `adapter.get_columns_in_relation`.

---

## **8. CONTROL FLOW — THE MOST POWERFUL PATTERN**

Macros that *generate SQL* from configuration are dbt's superpower. Example: a macro that emits a UNION ALL across many tables.

`macros/union_partitions.sql`:

```sql
{% macro union_partitions(table_prefix, partitions) %}
    {%- for p in partitions %}
        select * from {{ table_prefix }}_{{ p }}
        {%- if not loop.last %} union all {% endif -%}
    {%- endfor -%}
{% endmacro %}
```

Use:

```sql
{{ union_partitions('events', partitions=[202601, 202602, 202603]) }}
```

Compiles to:

```sql
select * from events_202601
union all
select * from events_202602
union all
select * from events_202603
```

This is how the `dbt_utils.union_relations` macro works — it builds a normalized union from a list of relations with different schemas.

---

## **9. ADAPTER DISPATCH — WAREHOUSE-AWARE MACROS**

Some logic differs per warehouse: `dateadd`, JSON path syntax, etc. dbt uses **dispatch**:

```sql
-- macros/date_diff.sql
{% macro date_diff(date_part, start_date, end_date) %}
    {{ adapter.dispatch('date_diff') (date_part, start_date, end_date) }}
{% endmacro %}

{% macro default__date_diff(date_part, start_date, end_date) %}
    datediff({{ date_part }}, {{ start_date }}, {{ end_date }})
{% endmacro %}

{% macro bigquery__date_diff(date_part, start_date, end_date) %}
    date_diff({{ end_date }}, {{ start_date }}, {{ date_part }})
{% endmacro %}

{% macro duckdb__date_diff(date_part, start_date, end_date) %}
    date_diff('{{ date_part }}', {{ start_date }}, {{ end_date }})
{% endmacro %}
```

`adapter.dispatch('date_diff')` looks up `<adapter>__date_diff` (e.g. `bigquery__date_diff`); falls back to `default__date_diff` if not found. Same usage in models, different SQL emitted per warehouse.

This is how `dbt_utils` ships one macro that works on every adapter.

---

## **10. BUILD ALONG**

### **Step 1.** Create `macros/safe_divide.sql`:

```sql
{% macro safe_divide(numerator, denominator, default=0) %}
    case when {{ denominator }} = 0 then {{ default }}
         else {{ numerator }} / {{ denominator }}::float
    end
{% endmacro %}
```

### **Step 2.** Use it in a model. Add `models/marts/customer_avg_order.sql`:

```sql
{{ config(materialized='table') }}

select
    customer_id,
    name,
    orders_count,
    lifetime_value,
    {{ safe_divide('lifetime_value', 'orders_count') }} as avg_order_value
from {{ ref('fct_customer_orders') }}
```

### **Step 3.** Inspect the compiled SQL **before** running:

```bash
dbt compile --select customer_avg_order
cat target/compiled/shop_dbt/models/marts/customer_avg_order.sql
```

You'll see:

```sql
select
    customer_id,
    name,
    orders_count,
    lifetime_value,
    case when orders_count = 0 then 0
         else lifetime_value / orders_count::float
    end as avg_order_value
from "shop"."main"."fct_customer_orders"
```

`dbt compile` is invaluable: it renders Jinja without running anything. Use it constantly to debug macros.

### **Step 4.** Now run:

```bash
dbt run --select customer_avg_order
```

### **Step 5.** Verify:

```bash
duckdb /tmp/dbt_workspace/shop.duckdb \
  "SELECT customer_id, orders_count, avg_order_value FROM main.customer_avg_order ORDER BY 1"
```

```
┌─────────────┬──────────────┬─────────────────┐
│ customer_id │ orders_count │ avg_order_value │
├─────────────┼──────────────┼─────────────────┤
│      1      │      3       │      106.50     │
│      2      │      1       │      102.00     │
│      3      │      1       │        8.75     │
│      4      │      0       │        0.00     │ ← Dan didn't order, no divide-by-zero
└─────────────┴──────────────┴─────────────────┘
```

The `safe_divide` macro saved a divide-by-zero on customer 4.

---

## **11. INTROSPECTING WITH `run_query`**

Sometimes you need to query the warehouse *during compile* to make a decision. `run_query` is the tool.

```sql
-- macros/get_active_partitions.sql
{% macro get_active_partitions() %}
    {%- set query -%}
        select distinct event_month from {{ ref('stg_events') }}
        where event_month >= current_date - interval '90 days'
    {%- endset -%}

    {%- if execute -%}
        {%- set results = run_query(query) -%}
        {%- set partitions = results.columns[0].values() -%}
        {{ return(partitions) }}
    {%- endif -%}
{% endmacro %}
```

`{%- if execute -%}` — only run during the actual run phase, not during parse. Without this gate, dbt would run the query during parse, which is slow and breaks if the table doesn't exist yet.

`run_query` returns a `Result` object with `.columns`, `.rows`, `.column_names`. Use it to drive subsequent code generation.

---

## **12. LOGGING AND ERROR HANDLING**

```jinja
{{ log('Starting transformation', info=True) }}

{% if some_bad_condition %}
    {{ exceptions.raise_compiler_error('Bad input: ' ~ some_bad_value) }}
{% endif %}

{% if some_warning_condition %}
    {{ exceptions.warn('Heads up: ...') }}
{% endif %}
```

`log(msg, info=True)` writes to stdout. Without `info=True` it's silent unless you `--log-level debug`.

`raise_compiler_error` aborts the compile cleanly with your message.

---

## **13. REAL-WORLD USE CASES**

- **Currency conversion** as a macro — every monetary column wraps `{{ to_usd(amount, currency) }}`.
- **Surrogate key generation** with `dbt_utils.generate_surrogate_key(['col1', 'col2'])` (hashes columns into a deterministic ID).
- **Audit columns macro** — every model adds `{{ audit_columns() }}` which emits `current_timestamp as updated_at, '{{ invocation_id }}' as run_id`.
- **Multi-tenant filtering** — a `tenant_filter()` macro that injects `where tenant_id = '{{ var("tenant") }}'` so the same models can be re-run per tenant.
- **GDPR redaction** — a macro that wraps PII columns: `{{ redact_in_dev('email') }}` returns `email` in prod, `'[redacted]'` in dev.

---

## **14. BEST PRACTICES & ANTI-PATTERNS**

### **Best practices**

- **Macros are functions — keep them small and composable.** A macro doing 3 things is a code smell.
- **Document macros with docstring-style comments** at the top of the file.
- **Prefer `dbt_utils` and `dbt_expectations` over hand-rolled macros.** Battle-tested, multi-adapter, well-documented.
- **Use `dbt compile` constantly.** When a macro misbehaves, the compiled output tells you why.
- **Use `adapter.dispatch` for cross-warehouse logic.**

### **Anti-patterns**

- **Macros that wrap a single SQL function** with no parameter logic. `{% macro upper_name(c) %}upper({{ c }}){% endmacro %}` is just `upper()` with extra steps.
- **Heavy logic in models** that should be macros. If you copy-paste a 20-line `case when` into 5 models, that's a macro.
- **Macros that hide complex side effects.** A macro should be a pure function of its inputs. If it talks to the warehouse, do it via `run_query` and gate with `execute`.
- **Forgetting whitespace control.** Compiled SQL with 30 blank lines between statements is correct but unreadable.
- **Macros calling other macros four levels deep.** Debugging compile errors becomes a stack-trace nightmare.

---

## **15. INTERVIEW QUESTIONS**

### **Q1. [Foundational] What's Jinja and why does dbt use it?**

**Model answer:** Jinja is a Python-based string-templating language. dbt uses it to add control flow (loops, conditionals), variables, functions, and macros to SQL — things SQL itself doesn't have. Jinja runs at compile time *before* SQL is sent to the warehouse, so the warehouse never sees `{% for %}` or `{{ }}`. The end result is plain warehouse SQL with whatever templating Jinja resolved out. dbt-specific functions like `ref()`, `source()`, `config()`, and `is_incremental()` are all Jinja macros provided by dbt.

### **Q2. [Foundational] What's the difference between `{{ }}` and `{% %}` in Jinja?**

**Model answer:** `{{ }}` evaluates an expression and **renders the result as text** into the output. `{% %}` is a **control statement** — `if`, `for`, `set`, `macro` — that doesn't produce output, just controls flow. So `{{ ref('x') }}` outputs the table name; `{% if x %}…{% endif %}` controls whether a block renders. `{# … #}` is a comment, removed at compile.

### **Q3. [Intermediate] When and why would you write a custom macro?**

**Model answer:** When the same SQL pattern appears in 3+ places. Examples: a `safe_divide` that protects against div-by-zero, a `cents_to_dollars` that normalizes monetary columns, a `tenant_filter` that injects multi-tenant predicates. Macros are dbt's DRY mechanism. They take args, render SQL, and are called from models with `{{ macro_name(args) }}`. Before writing one, check `dbt_utils` and `dbt_expectations` — many common patterns are already there.

### **Q4. [Intermediate] What's `run_query` and why does `if execute` matter when using it?**

**Model answer:** `run_query` executes a SQL query against the warehouse during compile and returns the result, so a macro can adapt its SQL based on real warehouse state. `execute` is a Jinja variable that's `false` during the parse phase (when dbt builds the manifest) and `true` during run. You gate `run_query` calls with `{% if execute %}` so they don't fire during parse — parse is supposed to be cheap and offline-safe, and querying the warehouse during parse can fail (table doesn't exist yet) or be wasteful. The gate makes the macro parse-safe and run-correct.

### **Q5. [Advanced] How does `adapter.dispatch` work, and when would you use it?**

**Model answer:** `adapter.dispatch('name')` looks up a macro using a fallback chain: `<adapter>__name` (e.g. `snowflake__name`) → `<package>__name` → `default__name`. Used to write macros whose implementation differs per warehouse — date functions, JSON path syntax, regex syntax. The pattern is: define a public macro that calls `adapter.dispatch`, then provide one or more `<adapter>__macro` implementations. This is how `dbt_utils` ships one macro that works across every adapter — each adapter contributes its own implementation, and dispatch routes the call.

### **Q6. [Advanced] You have a macro that should generate a different SQL fragment depending on whether a column exists in the source. How would you implement it?**

**Model answer:**

```sql
{% macro select_if_exists(relation, column_name, fallback) %}
    {%- if execute -%}
        {%- set cols = adapter.get_columns_in_relation(relation) -%}
        {%- set names = cols | map(attribute='name') | list -%}
        {%- if column_name in names -%}
            {{ column_name }}
        {%- else -%}
            {{ fallback }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
```

`adapter.get_columns_in_relation` introspects the warehouse during compile (gated with `if execute`). The macro emits the real column name if present, else the fallback expression. Useful for graceful schema evolution: an upstream column rename doesn't break models.

---

## **16. GOTCHAS**

- **Whitespace.** `{% set x = 1 %}` leaves a newline in the output. Use `{%- set x = 1 -%}` to trim. Whitespace bugs make compiled SQL ugly but rarely break it.
- **`return()` inside macros.** Macros that should return a value (not text) need `{{ return(value) }}` and to be called from `{% set y = my_macro() %}` not `{{ my_macro() }}`.
- **Quoting.** Jinja doesn't auto-quote strings. `where x = {{ var('y') }}` produces `where x = hello` (broken). Use `where x = '{{ var("y") }}'` to wrap in quotes — or use `dbt_utils.string_literal()`.
- **Parse-time vs run-time.** `if execute` matters whenever the macro queries the warehouse. Without the gate, parsing fails on a fresh project.
- **Macro order doesn't matter.** dbt scans all macros files and resolves them. You don't need to "import" macros to use them.
- **Cross-package macro shadowing.** If two packages define `my_macro`, dbt picks one and warns. Use `{{ package_name.my_macro() }}` to disambiguate.
- **Jinja errors point at compiled line numbers**, not your source. `dbt compile` to see the rendered output and find the issue.

---

## **NEXT STEP**

You can DRY up code with macros. Now learn how to capture history of changing data — snapshots — and load static reference data — seeds.

Go to [`06_snapshots_and_seeds.md`](06_snapshots_and_seeds.md).
