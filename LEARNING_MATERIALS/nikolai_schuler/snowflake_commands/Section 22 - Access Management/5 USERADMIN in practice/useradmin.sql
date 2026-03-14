-- =============================================================================
-- SECTION 22.5: USERADMIN IN PRACTICE
-- =============================================================================
--
-- OBJECTIVE:
--   Learn how USERADMIN creates users, assigns roles, and fixes role hierarchy
--   gaps by connecting orphaned roles to SYSADMIN.
--
-- WHAT YOU WILL LEARN:
--   1. USERADMIN's scope: users and roles only
--   2. Creating users and assigning existing roles
--   3. Identifying and fixing orphaned role hierarchies
--   4. USERADMIN vs SECURITYADMIN vs ACCOUNTADMIN
--
-- ADMIN ROLE COMPARISON (CRITICAL INTERVIEW TOPIC):
-- ┌──────────────────┬──────────┬──────────┬──────────┬──────────────────┐
-- │ Capability       │USERADMIN │SECURITY- │SYSADMIN  │ ACCOUNTADMIN     │
-- │                  │          │ADMIN     │          │                  │
-- ├──────────────────┼──────────┼──────────┼──────────┼──────────────────┤
-- │ Create users     │ ✓        │ ✓        │ ✗        │ ✓               │
-- │ Create roles     │ ✓        │ ✓        │ ✗        │ ✓               │
-- │ Grant roles      │ ✓        │ ✓        │ ✗        │ ✓               │
-- │ Manage grants    │ ✗        │ ✓        │ ✗        │ ✓               │
-- │ Create databases │ ✗        │ ✗        │ ✓        │ ✓               │
-- │ Create warehouses│ ✗        │ ✗        │ ✓        │ ✓               │
-- │ Manage billing   │ ✗        │ ✗        │ ✗        │ ✓               │
-- │ Resource monitors│ ✗        │ ✗        │ ✗        │ ✓               │
-- └──────────────────┴──────────┴──────────┴──────────┴──────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - USERADMIN is the most limited admin role: users and roles ONLY
--   - USERADMIN is a child of SECURITYADMIN in the default hierarchy
--   - USERADMIN cannot manage databases, warehouses, or object privileges
--   - Use USERADMIN when separation of duties requires a dedicated user manager
--   - SHOW ROLES displays all roles visible to the current role
-- =============================================================================


-- =============================================
-- STEP 1: Create a user with USERADMIN
-- =============================================
-- USERADMIN can create users and assign existing roles

-- User 4: Assign to the HR department
CREATE USER ben PASSWORD = '123'
DEFAULT_ROLE = ACCOUNTADMIN
MUST_CHANGE_PASSWORD = TRUE;

-- Assign the hr_admin role to ben
GRANT ROLE HR_ADMIN TO USER ben;

-- NOTE: USERADMIN can assign ANY existing role to a user,
-- even roles it didn't create (like HR_ADMIN)


-- =============================================
-- STEP 2: Inspect the current role hierarchy
-- =============================================
SHOW ROLES;
-- Key columns to examine:
--   name:           Role name
--   assigned_to_users: Number of users with this role
--   granted_to_roles:  Number of parent roles
--   granted_roles:     Number of child roles
--   owner:            Who owns/created the role

-- Look for roles where granted_to_roles = 0 (orphaned — not connected to SYSADMIN)


-- =============================================
-- STEP 3: Fix the orphaned HR role hierarchy
-- =============================================
-- In Section 22.2, hr_admin was intentionally NOT granted to SYSADMIN
-- This means SYSADMIN cannot see HR objects — let's fix this now

GRANT ROLE HR_ADMIN TO ROLE SYSADMIN;

-- Now the hierarchy is correct:
--   hr_users → hr_admin → SYSADMIN → ACCOUNTADMIN
-- SYSADMIN can now see and manage HR databases and objects


-- =============================================
-- STEP 4: Verify the fix
-- =============================================
SHOW ROLES;
-- hr_admin should now show granted_to_roles = 1 (SYSADMIN)


-- =============================================================================
-- CONCEPT GAP: SEPARATION OF DUTIES MODEL
-- =============================================================================
-- In a well-designed Snowflake account, responsibilities are divided:
--
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  WHO                │  DOES WHAT                                      │
-- ├─────────────────────┼─────────────────────────────────────────────────┤
-- │  ACCOUNTADMIN       │  Account-level: billing, resource monitors,    │
-- │  (1-2 people)       │  data shares, account parameters               │
-- │                     │                                                 │
-- │  SECURITYADMIN      │  Security: role hierarchies, cross-dept grants,│
-- │  (2-3 people)       │  masking policies, row access policies         │
-- │                     │                                                 │
-- │  USERADMIN          │  People: create users, assign roles,           │
-- │  (HR/IT team)       │  password resets, user deactivation            │
-- │                     │                                                 │
-- │  SYSADMIN           │  Infrastructure: warehouses, databases,        │
-- │  (DBA/DevOps team)  │  schemas, ownership transfers                  │
-- │                     │                                                 │
-- │  Custom admin roles │  Department: table management, data loading,   │
-- │  (dept leads)       │  granting access within their scope            │
-- │                     │                                                 │
-- │  Custom user roles  │  Daily work: query data, run reports,          │
-- │  (analysts/devs)    │  limited DML as granted                        │
-- └─────────────────────┴─────────────────────────────────────────────────┘
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: COMMON ACCESS MANAGEMENT INTERVIEW QUESTIONS
-- =============================================================================
-- Q1: "What happens if you don't connect custom roles to SYSADMIN?"
-- A: Objects created by those roles become invisible to SYSADMIN. Only
--    ACCOUNTADMIN can see/manage them, breaking separation of duties.
--
-- Q2: "Can a user have multiple roles? How do they switch?"
-- A: Yes. A user can be granted many roles. They switch with:
--    USE ROLE <role_name>;
--    Only ONE role is active at a time. The DEFAULT_ROLE is set at login.
--
-- Q3: "What's the difference between GRANT ROLE TO ROLE vs TO USER?"
-- A: GRANT ROLE child TO ROLE parent → builds role HIERARCHY (inheritance)
--    GRANT ROLE role TO USER user → ASSIGNS the role to a person
--
-- Q4: "How do you audit who has access to what?"
-- A: SHOW GRANTS TO ROLE <role>;       -- privileges the role has
--    SHOW GRANTS ON TABLE <table>;     -- who can access the table
--    SHOW GRANTS TO USER <user>;       -- roles assigned to the user
--
-- Q5: "What is the PUBLIC role?"
-- A: A special role automatically granted to every user. Granting privileges
--    to PUBLIC gives access to ALL users in the account. Use sparingly.
--
-- Q6: "Can SYSADMIN create users?"
-- A: No. SYSADMIN manages objects (warehouses, databases). User management
--    requires USERADMIN, SECURITYADMIN, or ACCOUNTADMIN.
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: COMPLETE RBAC SETUP CHECKLIST
-- =============================================================================
-- When setting up a new Snowflake account:
--
-- 1. PLAN THE ROLE HIERARCHY:
--    - Identify departments and access tiers
--    - Design admin/user role pairs per department
--    - Map the hierarchy up to SYSADMIN
--
-- 2. CREATE ROLES (as SECURITYADMIN):
--    CREATE ROLE dept_admin;
--    CREATE ROLE dept_users;
--    GRANT ROLE dept_users TO ROLE dept_admin;
--    GRANT ROLE dept_admin TO ROLE SYSADMIN;  -- CRITICAL!
--
-- 3. CREATE INFRASTRUCTURE (as SYSADMIN):
--    CREATE WAREHOUSE ...;
--    CREATE DATABASE ...;
--    GRANT OWNERSHIP ON DATABASE ... TO ROLE dept_admin;
--
-- 4. CREATE USERS (as USERADMIN):
--    CREATE USER ... DEFAULT_ROLE = dept_users;
--    GRANT ROLE dept_users TO USER ...;
--
-- 5. GRANT PRIVILEGES (as dept_admin):
--    GRANT USAGE ON DATABASE ... TO ROLE dept_users;
--    GRANT USAGE ON SCHEMA ... TO ROLE dept_users;
--    GRANT SELECT ON TABLE ... TO ROLE dept_users;
--    GRANT SELECT ON FUTURE TABLES IN SCHEMA ... TO ROLE dept_users;
--
-- 6. APPLY SECURITY POLICIES (as ACCOUNTADMIN):
--    CREATE MASKING POLICY ...;
--    ALTER TABLE ... SET MASKING POLICY ...;
-- =============================================================================
