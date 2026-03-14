-- =============================================================================
-- SECTION 21.3: ALTERING EXISTING MASKING POLICIES
-- =============================================================================
--
-- OBJECTIVE:
--   Learn to modify masking policy logic in place using ALTER MASKING POLICY
--   without the need to unset and re-apply the policy on columns.
--
-- WHAT YOU WILL LEARN:
--   1. ALTER MASKING POLICY ... SET BODY syntax
--   2. How ALTER differs from DROP/REPLACE workflow
--   3. When to use ALTER vs REPLACE
--   4. Verifying altered policy behavior
--
-- ALTER vs REPLACE COMPARISON (CRITICAL INTERVIEW TOPIC):
-- ┌──────────────────────┬────────────────────────┬──────────────────────────┐
-- │ Feature              │ ALTER ... SET BODY     │ DROP + REPLACE           │
-- ├──────────────────────┼────────────────────────┼──────────────────────────┤
-- │ Unset from columns?  │ NO (stays attached)    │ YES (must UNSET all)     │
-- │ Re-apply needed?     │ NO (automatic)         │ YES (must re-SET all)    │
-- │ Changes signature?   │ NO (body only)         │ YES (can change types)   │
-- │ Effect timing        │ Immediate              │ After re-apply           │
-- │ Risk of missed cols  │ None                   │ High (may forget re-SET) │
-- │ Use case             │ Change masking logic   │ Change parameter types   │
-- └──────────────────────┴────────────────────────┴──────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - ALTER MASKING POLICY SET BODY modifies the CASE expression in place
--   - No need to UNSET from columns — changes apply instantly everywhere
--   - Much safer than DROP/REPLACE: no risk of forgetting to re-apply
--   - ALTER cannot change the input/output data types — only the body logic
--   - Use DROP/REPLACE only when you need to change the policy signature
-- =============================================================================


-- =============================================
-- STEP 1: Verify current masking behavior
-- =============================================
-- Check what ANALYST_MASKED sees with current policy
USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;
-- Currently shows: '##-###-##' for phone (or whatever the current mask is)

-- Switch back to admin to alter the policy
USE ROLE ACCOUNTADMIN;


-- =============================================
-- STEP 2: ALTER the masking policy body
-- =============================================
-- Change the mask format from '##-###-##' to '**-**-**'
-- NOTE: "val" refers to the original parameter name defined in CREATE
ALTER MASKING POLICY phone SET BODY ->
    CASE
        WHEN current_role() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val
        ELSE '**-**-**'
    END;

-- KEY POINTS:
--   - SET BODY -> : replaces only the CASE expression
--   - "val" must match the parameter name from the original CREATE statement
--   - The policy remains attached to all columns — no re-apply needed
--   - Changes take effect IMMEDIATELY for all subsequent queries


-- =============================================
-- STEP 3: Verify the altered behavior
-- =============================================
USE ROLE ANALYST_MASKED;
SELECT * FROM CUSTOMERS;
-- NOW shows: '**-**-**' for phone (the new mask format)
-- The change happened instantly without any UNSET/SET cycle


-- =============================================
-- STEP 4: Clean up — UNSET if needed
-- =============================================
USE ROLE ACCOUNTADMIN;

-- UNSET removes the policy from a column (separate from ALTER)
ALTER TABLE CUSTOMERS MODIFY COLUMN email UNSET MASKING POLICY;


-- =============================================================================
-- CONCEPT GAP: ALTER MASKING POLICY BEST PRACTICES
-- =============================================================================
-- 1. ALWAYS use ALTER SET BODY when only changing masking logic:
--    - Faster (single command vs multi-step workflow)
--    - Safer (no risk of leaving columns unprotected during transition)
--    - Simpler (no need to track and re-apply to all columns)
--
-- 2. Use DROP + REPLACE only when:
--    - Changing the input parameter type (e.g., VARCHAR to NUMBER)
--    - Changing the return type
--    - Renaming the policy
--    - Moving the policy to a different schema
--
-- 3. AUDIT before altering:
--    - Check all affected columns first:
--      SELECT * FROM TABLE(information_schema.policy_references(
--          policy_name => 'phone'));
--    - Verify the new logic with a test role before deploying
--
-- 4. VERSION CONTROL:
--    - Keep masking policy definitions in source control (Git)
--    - Use ALTER in deployments to update policies without downtime
--    - Document each change with the reason for the mask modification
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: COMMON ALTER PATTERNS
-- =============================================================================
-- PATTERN 1: Add a new role to the allow list
--   ALTER MASKING POLICY phone SET BODY ->
--       CASE
--           WHEN current_role() IN ('ANALYST_FULL', 'ACCOUNTADMIN', 'NEW_ROLE')
--               THEN val
--           ELSE '**-**-**'
--       END;
--
-- PATTERN 2: Change from full mask to partial mask
--   ALTER MASKING POLICY phone SET BODY ->
--       CASE
--           WHEN current_role() IN ('ANALYST_FULL') THEN val
--           WHEN current_role() IN ('ANALYST_MASKED')
--               THEN CONCAT('***-***-', RIGHT(val, 4))  -- show last 4 digits
--           ELSE '***-***-****'
--       END;
--
-- PATTERN 3: Switch from static mask to hash
--   ALTER MASKING POLICY phone SET BODY ->
--       CASE
--           WHEN current_role() IN ('ANALYST_FULL') THEN val
--           ELSE SHA2(val)  -- consistent hash for pseudonymization
--       END;
-- =============================================================================
