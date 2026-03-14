-- =============================================================================
-- SECTION 22.2: SECURITYADMIN IN PRACTICE
-- =============================================================================
--
-- OBJECTIVE:
--   Learn how SECURITYADMIN creates custom role hierarchies, assigns roles
--   to users, and follows the best practice of connecting roles to SYSADMIN.
--
-- WHAT YOU WILL LEARN:
--   1. Creating custom roles with SECURITYADMIN
--   2. Building role hierarchies (parent-child relationships)
--   3. The critical best practice of granting custom roles to SYSADMIN
--   4. What happens when roles are NOT connected to SYSADMIN
--
-- ROLE HIERARCHY EXAMPLE (BUILT IN THIS SECTION):
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │                         ACCOUNTADMIN                                   │
-- │                        /           \                                   │
-- │                 SECURITYADMIN     SYSADMIN                            │
-- │                       |          /       \                             │
-- │                  USERADMIN  sales_admin   (hr_admin NOT connected!)   │
-- │                                 |              |                      │
-- │                            sales_users     hr_users                   │
-- │                                                                       │
-- │  ⚠ hr_admin is NOT granted to SYSADMIN = SYSADMIN cannot see its    │
-- │    objects. This is AGAINST best practice (intentional demo).         │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - SECURITYADMIN manages roles and users (CREATE ROLE, GRANT ROLE)
--   - Role hierarchies: child role → parent role (parent inherits child's privs)
--   - ALWAYS grant custom roles to SYSADMIN for object visibility
--   - If a role is NOT in SYSADMIN's hierarchy, SYSADMIN cannot manage its objects
--   - SECURITYADMIN inherits USERADMIN capabilities
--   - Each department should have admin + user role pairs
-- =============================================================================


-- =============================================
-- STEP 1: Create roles for Sales department
-- =============================================
-- Two-tier structure: admin role manages user role
CREATE ROLE sales_admin;
CREATE ROLE sales_users;

-- Build the hierarchy: sales_users → sales_admin
-- This means sales_admin inherits all privileges of sales_users
GRANT ROLE sales_users TO ROLE sales_admin;

-- BEST PRACTICE: Connect sales_admin to SYSADMIN
-- This allows SYSADMIN to manage objects owned by sales_admin
GRANT ROLE sales_admin TO ROLE SYSADMIN;


-- =============================================
-- STEP 2: Create users for Sales department
-- =============================================
-- Regular sales user
CREATE USER simon_sales PASSWORD = '123'
DEFAULT_ROLE = sales_users
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE sales_users TO USER simon_sales;

-- Sales administrator
CREATE USER olivia_sales_admin PASSWORD = '123'
DEFAULT_ROLE = sales_admin
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE sales_admin TO USER olivia_sales_admin;


-- =============================================
-- STEP 3: Create roles for HR department
-- =============================================
CREATE ROLE hr_admin;
CREATE ROLE hr_users;

-- Build the hierarchy: hr_users → hr_admin
GRANT ROLE hr_users TO ROLE hr_admin;

-- INTENTIONALLY NOT granting hr_admin to SYSADMIN (ANTI-PATTERN!)
-- Uncomment the line below to follow best practice:
-- GRANT ROLE hr_admin TO ROLE SYSADMIN;

-- This means SYSADMIN CANNOT:
--   - See databases/tables owned by hr_admin
--   - Grant access to hr_admin's objects
--   - Drop or modify hr_admin's objects
-- Only ACCOUNTADMIN or SECURITYADMIN can fix this hierarchy gap


-- =============================================
-- STEP 4: Create users for HR department
-- =============================================
-- Regular HR user
CREATE USER oliver_hr PASSWORD = '123'
DEFAULT_ROLE = hr_users
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE hr_users TO USER oliver_hr;

-- HR administrator
CREATE USER mike_hr_admin PASSWORD = '123'
DEFAULT_ROLE = hr_admin
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE hr_admin TO USER mike_hr_admin;


-- =============================================================================
-- CONCEPT GAP: WHY CONNECT CUSTOM ROLES TO SYSADMIN?
-- =============================================================================
-- SCENARIO: hr_admin creates a database called HR_DB
--
-- WITHOUT hr_admin → SYSADMIN connection:
--   USE ROLE SYSADMIN;
--   SHOW DATABASES;          -- HR_DB does NOT appear!
--   DROP DATABASE HR_DB;     -- ERROR: insufficient privileges
--   GRANT SELECT ON ...;     -- ERROR: insufficient privileges
--
--   Only ACCOUNTADMIN can see/manage HR_DB (breaks separation of duties)
--
-- WITH hr_admin → SYSADMIN connection:
--   USE ROLE SYSADMIN;
--   SHOW DATABASES;          -- HR_DB appears (inherited from hr_admin)
--   DROP DATABASE HR_DB;     -- Works (SYSADMIN inherits hr_admin privileges)
--
-- INTERVIEW ANSWER: "All custom roles should be granted to SYSADMIN so that
--   SYSADMIN can manage all objects in the account. Without this connection,
--   objects become orphaned and only ACCOUNTADMIN can manage them."
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: ROLE HIERARCHY DESIGN PATTERNS
-- =============================================================================
-- PATTERN 1: DEPARTMENT-BASED (shown above)
--   sales_users → sales_admin → SYSADMIN
--   hr_users    → hr_admin    → SYSADMIN
--
-- PATTERN 2: DATA ACCESS TIERS
--   data_reader → data_writer → data_admin → SYSADMIN
--
-- PATTERN 3: ENVIRONMENT-BASED
--   dev_user → dev_admin → SYSADMIN
--   prod_reader → prod_writer → prod_admin → SYSADMIN
--
-- GOLDEN RULES:
--   1. Every custom role must eventually connect to SYSADMIN
--   2. Use GRANT ROLE <child> TO ROLE <parent> for hierarchy
--   3. Use GRANT ROLE <role> TO USER <user> for user assignment
--   4. A user can have multiple roles but only ONE active at a time
--   5. USE ROLE <name> switches the active role during a session
-- =============================================================================
