-- =============================================================================
-- SECTION 22.1: ACCOUNTADMIN IN PRACTICE
-- =============================================================================
--
-- OBJECTIVE:
--   Understand the ACCOUNTADMIN role — the highest-level role in Snowflake —
--   and learn how to create users and assign system-defined roles.
--
-- WHAT YOU WILL LEARN:
--   1. What ACCOUNTADMIN is and what it can do
--   2. Creating users with CREATE USER
--   3. Assigning system-defined roles to users
--   4. Best practices for ACCOUNTADMIN usage
--
-- SNOWFLAKE DEFAULT ROLE HIERARCHY (CRITICAL INTERVIEW TOPIC):
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │                      ACCOUNTADMIN                                      │
-- │                     /            \                                     │
-- │               SECURITYADMIN     SYSADMIN                              │
-- │                    |                                                   │
-- │               USERADMIN                                               │
-- │                    |                                                   │
-- │                 PUBLIC  (granted to every user automatically)          │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- SYSTEM-DEFINED ROLES COMPARISON:
-- ┌──────────────────┬────────────────────────────────────────────────────┐
-- │ Role             │ Primary Responsibilities                          │
-- ├──────────────────┼────────────────────────────────────────────────────┤
-- │ ACCOUNTADMIN     │ Top-level role. Billing, resource monitors,      │
-- │                  │ account-level params. SYSADMIN + SECURITYADMIN.  │
-- ├──────────────────┼────────────────────────────────────────────────────┤
-- │ SECURITYADMIN    │ Manage roles, grants, and security policies.     │
-- │                  │ Inherits USERADMIN capabilities.                  │
-- ├──────────────────┼────────────────────────────────────────────────────┤
-- │ SYSADMIN         │ Create/manage warehouses, databases, schemas.    │
-- │                  │ The "infrastructure admin" role.                  │
-- ├──────────────────┼────────────────────────────────────────────────────┤
-- │ USERADMIN        │ Create/manage users and roles only.              │
-- │                  │ Cannot manage objects or grants.                  │
-- ├──────────────────┼────────────────────────────────────────────────────┤
-- │ PUBLIC           │ Automatically granted to every user.             │
-- │                  │ Lowest privilege level.                           │
-- └──────────────────┴────────────────────────────────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - ACCOUNTADMIN = SYSADMIN + SECURITYADMIN combined
--   - ACCOUNTADMIN should NEVER be a user's DEFAULT_ROLE
--   - Limit ACCOUNTADMIN access to 1-2 trusted administrators
--   - MUST_CHANGE_PASSWORD = TRUE enforces password reset on first login
--   - ACCOUNTADMIN manages billing, resource monitors, and account parameters
--   - Use ACCOUNTADMIN only for account-level tasks, not daily operations
-- =============================================================================


-- =============================================
-- STEP 1: Create users with ACCOUNTADMIN
-- =============================================
-- ACCOUNTADMIN (or USERADMIN/SECURITYADMIN) can create users

-- User 1: Full administrator (gets ACCOUNTADMIN role)
CREATE USER maria PASSWORD = '123'
DEFAULT_ROLE = ACCOUNTADMIN
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE ACCOUNTADMIN TO USER maria;
-- WARNING: In production, NEVER set DEFAULT_ROLE = ACCOUNTADMIN
-- This is shown for demonstration only. Best practice is to use a lower role
-- as default and switch to ACCOUNTADMIN only when needed.


-- User 2: Security administrator
CREATE USER frank PASSWORD = '123'
DEFAULT_ROLE = SECURITYADMIN
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE SECURITYADMIN TO USER frank;
-- Frank can manage roles, grants, and users but NOT create warehouses/databases


-- User 3: System administrator
CREATE USER adam PASSWORD = '123'
DEFAULT_ROLE = SYSADMIN
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE SYSADMIN TO USER adam;
-- Adam can create warehouses and databases but NOT manage security


-- =============================================
-- STEP 2: Verify the user setup
-- =============================================
SHOW USERS;
-- Key columns: name, default_role, must_change_password, disabled


-- =============================================================================
-- CONCEPT GAP: ACCOUNTADMIN BEST PRACTICES (COMMON INTERVIEW QUESTIONS)
-- =============================================================================
-- 1. NEVER use ACCOUNTADMIN as DEFAULT_ROLE:
--    - Set DEFAULT_ROLE to SYSADMIN or a custom role
--    - Switch to ACCOUNTADMIN only when needed: USE ROLE ACCOUNTADMIN;
--
-- 2. LIMIT ACCOUNTADMIN access:
--    - Grant to maximum 2-3 users in the organization
--    - Enable MFA (multi-factor authentication) for all ACCOUNTADMIN users
--
-- 3. ACCOUNTADMIN-only operations:
--    - Viewing/modifying billing and usage information
--    - Creating and managing resource monitors
--    - Setting account-level parameters
--    - Managing data shares at the account level
--    - Viewing ACCOUNT_USAGE schema (SNOWFLAKE database)
--
-- 4. SEPARATION OF DUTIES:
--    - Use SYSADMIN for creating databases and warehouses
--    - Use SECURITYADMIN for managing roles and grants
--    - Use USERADMIN for creating users
--    - Reserve ACCOUNTADMIN for account-level tasks only
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: CREATE USER OPTIONS
-- =============================================================================
-- CREATE USER <name>
--   PASSWORD = '<password>'             -- Initial password
--   DEFAULT_ROLE = <role>               -- Role assumed on login
--   DEFAULT_WAREHOUSE = <warehouse>     -- Default warehouse for queries
--   DEFAULT_NAMESPACE = <db.schema>     -- Default database.schema
--   MUST_CHANGE_PASSWORD = TRUE/FALSE   -- Force password reset on first login
--   DISABLED = TRUE/FALSE               -- Create in disabled state
--   DAYS_TO_EXPIRY = <n>                -- Account expires after n days
--   MINS_TO_UNLOCK = <n>                -- Auto-unlock after n minutes
--   LOGIN_NAME = '<login>'              -- Different from display name
--   DISPLAY_NAME = '<display>'          -- Friendly name in UI
--   EMAIL = '<email>'                   -- Contact email
--   COMMENT = '<text>'                  -- Description/notes
--
-- IMPORTANT: Setting DEFAULT_ROLE does NOT grant that role to the user.
-- You must separately run: GRANT ROLE <role> TO USER <name>;
-- =============================================================================
