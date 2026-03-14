-- =============================================================================
-- SECTION 21.2: UNSETTING AND REPLACING MASKING POLICIES
-- =============================================================================
--
-- OBJECTIVE:
--   Learn how to remove masking policies from columns, replace policies with
--   updated versions, and inspect which columns have policies applied.
--
-- WHAT YOU WILL LEARN:
--   1. Applying a policy to multiple columns
--   2. Why you must UNSET before DROP/REPLACE
--   3. Using DESC, SHOW, and policy_references() for inspection
--   4. The correct workflow for replacing a masking policy
--
-- MASKING POLICY LIFECYCLE (CRITICAL INTERVIEW TOPIC):
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │                  POLICY MANAGEMENT WORKFLOW                             │
-- │                                                                        │
-- │  CREATE policy ──► SET on column(s) ──► UNSET from column(s) ──►      │
-- │                                          DROP or REPLACE policy        │
-- │                                                                        │
-- │  IMPORTANT: You CANNOT DROP or REPLACE a policy while it is           │
-- │  attached to ANY column. You must UNSET from ALL columns first.       │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- POLICY INSPECTION COMMANDS:
-- ┌──────────────────────────────────────┬─────────────────────────────────┐
-- │ Command                              │ What It Shows                  │
-- ├──────────────────────────────────────┼─────────────────────────────────┤
-- │ SHOW MASKING POLICIES               │ All policies in current schema │
-- │ DESC MASKING POLICY <name>          │ Policy definition details      │
-- │ policy_references(policy_name=>...) │ Columns using a specific policy│
-- └──────────────────────────────────────┴─────────────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - UNSET removes the policy from a column (does NOT drop the policy)
--   - A policy CANNOT be dropped while attached to any column
--   - One policy can be applied to multiple columns (same data type)
--   - information_schema.policy_references() shows all columns using a policy
--   - Always UNSET from ALL columns before DROP or CREATE OR REPLACE
-- =============================================================================


USE ROLE ACCOUNTADMIN;


-- =============================================
-- STEP 1: Apply a policy to multiple columns
-- =============================================
-- The same masking policy can be applied to any column with matching data type
-- Here we apply the "phone" policy (VARCHAR→VARCHAR) to the full_name column too
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name
SET MASKING POLICY phone;

-- Verify: the "phone" policy is now on BOTH phone and full_name columns
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN phone
SET MASKING POLICY phone;

-- NOTE: Both columns must be VARCHAR type since the policy expects VARCHAR input


-- =============================================
-- STEP 2: Attempt to DROP a policy while in use
-- =============================================
-- This will FAIL because the policy is still attached to columns
DROP MASKING POLICY phone;
-- Error: "Policy PHONE is currently attached to one or more columns"

-- You could try CREATE OR REPLACE but this also fails while policy is in use
CREATE OR REPLACE MASKING POLICY phone AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN current_role() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val
        ELSE CONCAT(LEFT(val, 2), '*******')
    END;
-- Same error: policy is attached


-- =============================================
-- STEP 3: Inspect policies and their assignments
-- =============================================
-- See the policy definition
DESC MASKING POLICY phone;
-- Shows: name, input type, return type, body (the CASE expression)

-- List all masking policies in the current schema
SHOW MASKING POLICIES;

-- Find ALL columns that have the "phone" policy applied
SELECT *
FROM TABLE(information_schema.policy_references(policy_name => 'phone'));
-- Returns: table_name, column_name, policy_name for every column using this policy
-- This is CRITICAL for knowing which columns to UNSET before dropping


-- =============================================
-- STEP 4: UNSET policy from all columns
-- =============================================
-- Remove the policy from full_name
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name
UNSET MASKING POLICY;

-- Remove the policy from email (if applied)
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN email
UNSET MASKING POLICY;

-- Remove the policy from phone
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN phone
UNSET MASKING POLICY;

-- NOTE: UNSET removes the policy association — it does NOT drop the policy object
-- The policy still exists and can be re-applied later


-- =============================================
-- STEP 5: Now replace the policy (after UNSET)
-- =============================================
-- Create a new policy with partial masking instead of full masking
CREATE OR REPLACE MASKING POLICY names AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN current_role() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val
        -- Show first 2 characters + asterisks (partial masking)
        ELSE CONCAT(LEFT(val, 2), '*******')
    END;


-- =============================================
-- STEP 6: Apply the new policy
-- =============================================
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN full_name
SET MASKING POLICY names;


-- =============================================
-- STEP 7: Validate the new masking behavior
-- =============================================
-- ANALYST_FULL sees real names
USE ROLE ANALYST_FULL;
SELECT * FROM CUSTOMERS;
-- Expected: full_name shows 'Lewiss MacDwyer', 'Ty Pettingall', etc.

-- ANALYST_MASKED sees partial names
USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;
-- Expected: full_name shows 'Le*******', 'Ty*******', etc.


-- =============================================================================
-- CONCEPT GAP: COMPLETE POLICY REPLACEMENT WORKFLOW
-- =============================================================================
-- CORRECT WORKFLOW to replace a masking policy:
--
--   1. Find all columns using the policy:
--      SELECT * FROM TABLE(information_schema.policy_references(
--          policy_name => 'my_policy'));
--
--   2. UNSET the policy from ALL columns:
--      ALTER TABLE t1 MODIFY COLUMN c1 UNSET MASKING POLICY;
--      ALTER TABLE t2 MODIFY COLUMN c2 UNSET MASKING POLICY;
--
--   3. DROP or REPLACE the policy:
--      CREATE OR REPLACE MASKING POLICY my_policy AS ...
--
--   4. Re-apply the new policy:
--      ALTER TABLE t1 MODIFY COLUMN c1 SET MASKING POLICY my_policy;
--      ALTER TABLE t2 MODIFY COLUMN c2 SET MASKING POLICY my_policy;
--
-- SHORTCUT: Use ALTER MASKING POLICY ... SET BODY (see Section 21.3)
--   to modify the policy logic WITHOUT needing to UNSET/re-apply!
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: FORCE REPLACE vs ALTER (INTERVIEW QUESTION)
-- =============================================================================
-- Q: "What's the fastest way to change a masking policy's logic?"
--
-- A: ALTER MASKING POLICY <name> SET BODY -> <new_expression>
--    This modifies the policy IN PLACE without needing to UNSET from columns.
--    Changes take effect immediately on all columns using the policy.
--    (Covered in detail in Section 21.3)
--
-- Q: "When would you use DROP/REPLACE instead of ALTER?"
--
-- A: When you need to change the parameter types (e.g., VARCHAR to NUMBER),
--    or when you want to completely restructure the policy. ALTER can only
--    change the BODY expression, not the signature.
-- =============================================================================
