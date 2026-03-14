-- =============================================================================
-- SECTION 21.1: CREATING A DYNAMIC DATA MASKING POLICY
-- =============================================================================
--
-- OBJECTIVE:
--   Learn how to create and apply dynamic data masking policies to protect
--   sensitive column data based on the querying user's role at runtime.
--
-- WHAT YOU WILL LEARN:
--   1. What dynamic data masking is and how it works
--   2. Creating masking policies with CASE expressions
--   3. Applying policies to table columns
--   4. Setting up roles for testing masking behavior
--
-- HOW DYNAMIC DATA MASKING WORKS (CRITICAL INTERVIEW TOPIC):
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │                    MASKING POLICY FLOW                                  │
-- │                                                                        │
-- │  User runs: SELECT phone FROM customers;                              │
-- │                                                                        │
-- │  1. Snowflake checks current_role() of the querying user              │
-- │  2. Finds the masking policy attached to the "phone" column           │
-- │  3. Evaluates the CASE expression in the policy                       │
-- │  4. Returns:                                                           │
-- │     - ANALYST_FULL  → '262-665-9168'  (real data)                     │
-- │     - ANALYST_MASKED → '##-###-##'     (masked data)                  │
-- │                                                                        │
-- │  The underlying data is NEVER modified — only the query result changes │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- MASKING POLICY CHARACTERISTICS:
-- ┌──────────────────────┬──────────────────────────────────────────────────┐
-- │ Feature              │ Description                                      │
-- ├──────────────────────┼──────────────────────────────────────────────────┤
-- │ Scope                │ Column-level (one policy per column)            │
-- │ Evaluation time      │ Query time (dynamic, not static)               │
-- │ Data modification    │ None (masks output only)                       │
-- │ Transparency         │ Users don't change their queries               │
-- │ Object level         │ Schema-level object                            │
-- │ Key function         │ current_role() determines visibility           │
-- │ Data type            │ Input and output types must match              │
-- │ Required privilege   │ ACCOUNTADMIN or CREATE MASKING POLICY          │
-- └──────────────────────┴──────────────────────────────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - Masking policies are schema-level objects applied to columns
--   - They use current_role() to determine what the user sees
--   - The underlying data is NEVER modified — only the query output changes
--   - Input and return data types must match (varchar→varchar, number→number)
--   - One column can have only ONE masking policy at a time
--   - A single policy can be applied to multiple columns of the same type
-- =============================================================================


-- =============================================
-- STEP 1: Set up the environment
-- =============================================
USE DEMO_DB;
USE ROLE ACCOUNTADMIN;


-- =============================================
-- STEP 2: Create a test table with sensitive data
-- =============================================
CREATE OR REPLACE TABLE customers (
    id          NUMBER,
    full_name   VARCHAR,
    email       VARCHAR,
    phone       VARCHAR,
    spent       NUMBER,
    create_date DATE DEFAULT CURRENT_DATE
);

-- Insert sample data with PII (personally identifiable information)
INSERT INTO customers (id, full_name, email, phone, spent)
VALUES
    (1, 'Lewiss MacDwyer',   'lmacdwyer0@un.org',           '262-665-9168', 140),
    (2, 'Ty Pettingall',     'tpettingall1@mayoclinic.com',  '734-987-7120', 254),
    (3, 'Marlee Spadazzi',   'mspadazzi2@txnews.com',        '867-946-3659', 120),
    (4, 'Heywood Tearney',   'htearney3@patch.com',          '563-853-8192', 1230),
    (5, 'Odilia Seti',       'oseti4@globo.com',             '730-451-8637', 143),
    (6, 'Meggie Washtell',   'mwashtell5@rediff.com',        '568-896-6138', 600);


-- =============================================
-- STEP 3: Create roles for testing
-- =============================================
-- ANALYST_FULL: sees all data (privileged role)
-- ANALYST_MASKED: sees masked data (restricted role)
CREATE OR REPLACE ROLE ANALYST_MASKED;
CREATE OR REPLACE ROLE ANALYST_FULL;


-- =============================================
-- STEP 4: Grant necessary privileges to roles
-- =============================================
-- Both roles need: USAGE on database + schema, SELECT on table, USAGE on warehouse
GRANT SELECT ON TABLE DEMO_DB.PUBLIC.CUSTOMERS TO ROLE ANALYST_MASKED;
GRANT SELECT ON TABLE DEMO_DB.PUBLIC.CUSTOMERS TO ROLE ANALYST_FULL;

GRANT USAGE ON SCHEMA DEMO_DB.PUBLIC TO ROLE ANALYST_MASKED;
GRANT USAGE ON SCHEMA DEMO_DB.PUBLIC TO ROLE ANALYST_FULL;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_MASKED;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_FULL;


-- =============================================
-- STEP 5: Assign roles to a user
-- =============================================
-- Replace NIKOLAISCHULER with your actual username
GRANT ROLE ANALYST_MASKED TO USER NIKOLAISCHULER;
GRANT ROLE ANALYST_FULL TO USER NIKOLAISCHULER;


-- =============================================
-- STEP 6: Create the masking policy
-- =============================================
-- Syntax: CREATE MASKING POLICY <name> AS (val <type>) RETURNS <type> -> <expression>
-- The policy takes a column value as input and returns masked or unmasked output
CREATE OR REPLACE MASKING POLICY phone
    AS (val VARCHAR) RETURNS VARCHAR ->
        CASE
            -- These roles see the real phone number
            WHEN current_role() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val
            -- All other roles see a masked placeholder
            ELSE '##-###-##'
        END;

-- KEY POINTS:
--   - (val VARCHAR): input parameter — receives the actual column value
--   - RETURNS VARCHAR: output type — must match the input type
--   - current_role(): built-in function returning the user's active role
--   - CASE expression: the masking logic (can have multiple WHEN clauses)


-- =============================================
-- STEP 7: Apply the policy to a column
-- =============================================
-- Attach the masking policy to the phone column
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN phone
SET MASKING POLICY PHONE;

-- NOTE: Only ONE masking policy can be active per column at a time
-- The policy name is case-insensitive in ALTER TABLE


-- =============================================
-- STEP 8: Validate the masking behavior
-- =============================================
-- Test with ANALYST_FULL role (should see real phone numbers)
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;
-- Expected: phone column shows '262-665-9168', '734-987-7120', etc.

-- Test with ANALYST_MASKED role (should see masked phone numbers)
USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;
-- Expected: phone column shows '##-###-##' for all rows


-- =============================================================================
-- CONCEPT GAP: MASKING POLICY DESIGN PATTERNS
-- =============================================================================
-- PATTERN 1: BINARY MASK (shown above)
--   Full data or fully masked. Simplest pattern.
--   Use case: Phone numbers, SSN, credit card numbers
--
-- PATTERN 2: TIERED ACCESS (multiple roles, different visibility)
--   CASE
--     WHEN current_role() IN ('ADMIN')     THEN val           -- full
--     WHEN current_role() IN ('ANALYST')   THEN LEFT(val, 3)  -- partial
--     ELSE '***'                                               -- none
--   END
--   Use case: Different departments need different levels of access
--
-- PATTERN 3: NULL MASKING (return NULL instead of placeholder)
--   CASE
--     WHEN current_role() IN ('FULL_ACCESS') THEN val
--     ELSE NULL
--   END
--   Use case: When masked users shouldn't know the data format
--
-- IMPORTANT INTERVIEW NOTES:
--   - Masking policies are evaluated BEFORE result caching
--   - Different roles querying the same table get different cached results
--   - Masking works with views, including secure views
--   - Masking policies cannot reference other tables (no subqueries)
--   - Use IS_ROLE_IN_SESSION() for role hierarchy-aware masking (advanced)
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: MASKING vs OTHER SECURITY MECHANISMS
-- =============================================================================
-- ┌──────────────────────┬────────────────────┬────────────────────────────┐
-- │ Mechanism            │ Scope              │ What It Controls           │
-- ├──────────────────────┼────────────────────┼────────────────────────────┤
-- │ Dynamic Data Masking │ Column-level       │ What VALUE users see       │
-- │ Row Access Policy    │ Row-level          │ Which ROWS users see       │
-- │ Secure View         │ View-level         │ Definition + data exposure │
-- │ RBAC (GRANT/REVOKE) │ Object-level       │ Which OBJECTS users access │
-- │ Network Policy       │ Account-level      │ Which IPs can connect     │
-- └──────────────────────┴────────────────────┴────────────────────────────┘
--
-- TIP: In a real-world scenario, you often combine ALL of these:
--   - RBAC controls which tables a role can access
--   - Row access policies filter which rows they see
--   - Masking policies control which column values are visible
--   - Secure views wrap everything for sharing
-- =============================================================================
