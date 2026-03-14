-- =============================================================================
-- SECTION 21.4: REAL-LIFE DYNAMIC DATA MASKING EXAMPLES
-- =============================================================================
--
-- OBJECTIVE:
--   Explore practical masking strategies used in production: partial email
--   masking with regex, SHA-2 hashing for pseudonymization, and date masking.
--
-- WHAT YOU WILL LEARN:
--   1. Partial masking with regexp_replace() for emails
--   2. SHA-2 hashing for consistent pseudonymization
--   3. Date masking with date_from_parts()
--   4. When to use each masking strategy
--
-- MASKING STRATEGIES COMPARISON (CRITICAL INTERVIEW TOPIC):
-- ┌───────────────────┬─────────────────────┬───────────────────────────────┐
-- │ Strategy          │ Function            │ Use Case                      │
-- ├───────────────────┼─────────────────────┼───────────────────────────────┤
-- │ Full replacement  │ Static string       │ Phone, SSN, credit card      │
-- │                   │ '##-###-##'         │ No context needed             │
-- ├───────────────────┼─────────────────────┼───────────────────────────────┤
-- │ Partial masking   │ regexp_replace(),   │ Email (keep domain),         │
-- │                   │ LEFT(), RIGHT()     │ Name (keep initials)          │
-- ├───────────────────┼─────────────────────┼───────────────────────────────┤
-- │ Hash (SHA-2)      │ SHA2(val)           │ Pseudonymization, analytics  │
-- │                   │                     │ Preserves JOIN capability     │
-- ├───────────────────┼─────────────────────┼───────────────────────────────┤
-- │ Date replacement  │ date_from_parts()   │ Birthdate, hire date         │
-- │                   │                     │ Replace with sentinel date   │
-- ├───────────────────┼─────────────────────┼───────────────────────────────┤
-- │ NULL masking      │ NULL                │ When masked users shouldn't  │
-- │                   │                     │ even see the data format      │
-- └───────────────────┴─────────────────────┴───────────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - regexp_replace() enables partial masking (e.g., hide email username)
--   - SHA2() produces consistent hashes (same input → same hash every time)
--   - SHA2 hashing preserves referential integrity for JOINs on masked data
--   - date_from_parts() replaces dates with a sentinel value
--   - Different columns can have different masking strategies
--   - Choose the strategy based on how much context users need
-- =============================================================================


USE ROLE ACCOUNTADMIN;


-- =============================================================================
-- EXAMPLE 1: PARTIAL EMAIL MASKING WITH regexp_replace()
-- =============================================================================
-- Goal: Hide the username part of email, keep the domain visible
-- 'lmacdwyer0@un.org' → '*****@un.org'
-- This preserves context (the domain) while hiding the identity

CREATE OR REPLACE MASKING POLICY emails AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        -- Full access role sees real emails
        WHEN current_role() IN ('ANALYST_FULL') THEN val
        -- Masked role sees username replaced with asterisks, domain preserved
        WHEN current_role() IN ('ANALYST_MASKED')
            THEN regexp_replace(val, '.+\@', '*****@')
            -- regex: '.+\@' matches everything before and including @
            -- replaces with '*****@' so domain remains visible
        -- All other roles see fully masked
        ELSE '********'
    END;

-- Apply to the email column
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN email
SET MASKING POLICY emails;

-- Validate: ANALYST_FULL sees real emails
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;
-- Expected: 'lmacdwyer0@un.org', 'tpettingall1@mayoclinic.com', etc.

-- Validate: ANALYST_MASKED sees partial emails
USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;
-- Expected: '*****@un.org', '*****@mayoclinic.com', etc.
-- Note: domain is visible but username is hidden

USE ROLE ACCOUNTADMIN;


-- =============================================================================
-- EXAMPLE 2: SHA-2 HASHING FOR PSEUDONYMIZATION
-- =============================================================================
-- Goal: Replace names with consistent hash values
-- 'Lewiss MacDwyer' → '7a3f2b...' (64-char hex string)
-- Same input ALWAYS produces the same hash → enables JOINs on masked data

CREATE OR REPLACE MASKING POLICY sha2 AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN current_role() IN ('ANALYST_FULL') THEN val
        -- SHA2 returns a 256-bit hash as a 64-character hex string
        ELSE SHA2(val)
    END;

-- Apply to the full_name column
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name
SET MASKING POLICY sha2;

-- Validate: ANALYST_FULL sees real names
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;
-- Expected: 'Lewiss MacDwyer', 'Ty Pettingall', etc.

-- Validate: ANALYST_MASKED sees hash values
USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;
-- Expected: '7a3f2b4c...', 'e8d91f2a...', etc. (64-char hex strings)

-- WHY SHA2 IS POWERFUL FOR ANALYTICS:
-- Even though the analyst can't see the real name, they CAN:
--   - COUNT DISTINCT customers (each name hashes uniquely)
--   - JOIN tables on the hashed name (same name → same hash)
--   - Track customer behavior over time (consistent hash)
--   - Group by customer without knowing their identity

-- Clean up: remove the sha2 policy from full_name
USE ROLE ACCOUNTADMIN;
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name
UNSET MASKING POLICY;

-- Validate cleanup
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;

USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;

USE ROLE ACCOUNTADMIN;


-- =============================================================================
-- EXAMPLE 3: DATE MASKING WITH date_from_parts()
-- =============================================================================
-- Goal: Replace real dates with a sentinel date for masked roles
-- '2024-03-15' → '0001-01-01' (obviously fake date signals masking)

CREATE OR REPLACE MASKING POLICY dates AS (val DATE) RETURNS DATE ->
    CASE
        WHEN current_role() IN ('ANALYST_FULL') THEN val
        -- Replace with sentinel date (year 0001 is clearly not real)
        ELSE date_from_parts(0001, 01, 01)::DATE
    END;

-- NOTE: This policy uses DATE type, not VARCHAR
-- Input type (DATE) must match return type (DATE)

-- Apply to the create_date column
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN create_date
SET MASKING POLICY dates;

-- Validate: ANALYST_FULL sees real dates
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;
-- Expected: create_date shows actual dates

-- Validate: ANALYST_MASKED sees sentinel date
USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;
-- Expected: create_date shows '0001-01-01' for all rows


-- =============================================================================
-- CONCEPT GAP: CHOOSING THE RIGHT MASKING STRATEGY
-- =============================================================================
-- DECISION TREE:
--
--   Does the masked user need ANY context from the data?
--     │
--     ├── NO → Full replacement ('***') or NULL
--     │         Best for: SSN, passwords, credit card numbers
--     │
--     └── YES → What kind of context?
--               │
--               ├── Partial format → regexp_replace() or LEFT()/RIGHT()
--               │   Best for: emails (keep domain), phones (keep area code)
--               │
--               ├── Analytical capability → SHA2() hash
--               │   Best for: names, IDs where you need GROUP BY / JOIN
--               │
--               └── Approximate value → ROUND(), DATE_TRUNC()
--                   Best for: salaries (round to nearest 10K), dates (year only)
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: ADVANCED MASKING PATTERNS FOR PRODUCTION
-- =============================================================================
-- PATTERN 1: Mask credit card numbers (show last 4 digits only)
--   CREATE MASKING POLICY cc_mask AS (val VARCHAR) RETURNS VARCHAR ->
--       CASE
--           WHEN current_role() IN ('FINANCE_ADMIN') THEN val
--           ELSE CONCAT('****-****-****-', RIGHT(val, 4))
--       END;
--
-- PATTERN 2: Mask salary with rounding (preserve analytics capability)
--   CREATE MASKING POLICY salary_mask AS (val NUMBER) RETURNS NUMBER ->
--       CASE
--           WHEN current_role() IN ('HR_ADMIN') THEN val
--           WHEN current_role() IN ('HR_ANALYST')
--               THEN ROUND(val, -4)  -- Round to nearest 10,000
--           ELSE 0
--       END;
--
-- PATTERN 3: Geographic masking (show country only, hide city)
--   CREATE MASKING POLICY geo_mask AS (val VARCHAR) RETURNS VARCHAR ->
--       CASE
--           WHEN current_role() IN ('GEO_ADMIN') THEN val
--           ELSE SPLIT_PART(val, ',', -1)  -- Keep last part (country)
--       END;
--
-- PATTERN 4: Time-based masking (mask records older than 1 year)
--   This requires combining masking policies with row access policies
--   or using conditional logic within the CASE expression based on
--   other column values (note: limited support in Snowflake).
-- =============================================================================
