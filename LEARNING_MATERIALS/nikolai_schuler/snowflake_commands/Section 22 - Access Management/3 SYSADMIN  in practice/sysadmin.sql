-- =============================================================================
-- SECTION 22.3: SYSADMIN IN PRACTICE
-- =============================================================================
--
-- OBJECTIVE:
--   Learn how SYSADMIN creates and manages warehouses, databases, and
--   transfers ownership of objects to departmental roles.
--
-- WHAT YOU WILL LEARN:
--   1. Creating warehouses with SYSADMIN
--   2. Creating databases and granting access
--   3. Transferring ownership to custom roles with GRANT OWNERSHIP
--   4. The PUBLIC role and broad access patterns
--
-- SYSADMIN RESPONSIBILITIES:
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  SYSADMIN CAN:                    │  SYSADMIN CANNOT:                 │
-- ├───────────────────────────────────┼────────────────────────────────────┤
-- │  ✓ Create warehouses             │  ✗ Create/manage users            │
-- │  ✓ Create databases & schemas    │  ✗ Create/manage roles            │
-- │  ✓ Create tables, views, etc.    │  ✗ Manage security policies       │
-- │  ✓ Grant object privileges       │  ✗ Manage billing/resource monitors│
-- │  ✓ Transfer ownership of objects │  ✗ See objects outside its        │
-- │  ✓ Manage objects in its         │    role hierarchy                  │
-- │    role hierarchy                │                                    │
-- └───────────────────────────────────┴────────────────────────────────────┘
--
-- GRANT OWNERSHIP FLOW:
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  SYSADMIN creates database ──► SYSADMIN owns it                      │
-- │  GRANT OWNERSHIP ... TO ROLE sales_admin ──► sales_admin owns it     │
-- │  sales_admin can now: CREATE TABLE, ALTER, DROP within that database  │
-- │  SYSADMIN can still see it (because sales_admin → SYSADMIN)          │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - SYSADMIN is the "infrastructure admin" — creates warehouses and databases
--   - GRANT OWNERSHIP transfers full control of objects to other roles
--   - After ownership transfer, the new owner has DDL rights on that object
--   - PUBLIC role is automatically granted to every user in the account
--   - Granting USAGE ON WAREHOUSE to PUBLIC gives all users query capability
--   - SYSADMIN can only manage objects owned by roles in its hierarchy
-- =============================================================================


-- =============================================
-- STEP 1: Create a shared warehouse
-- =============================================
-- Create a small warehouse available to everyone
CREATE WAREHOUSE public_wh WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300       -- Suspend after 5 minutes of inactivity
    AUTO_RESUME = TRUE;      -- Auto-resume when a query arrives

-- Grant usage to the PUBLIC role (every user gets access)
GRANT USAGE ON WAREHOUSE public_wh TO ROLE PUBLIC;
-- NOTE: PUBLIC role is special — every user inherits it automatically
-- This effectively gives ALL users access to this warehouse


-- =============================================
-- STEP 2: Create a shared database
-- =============================================
-- A database accessible to everyone (reference data, lookups, etc.)
CREATE DATABASE common_db;
GRANT USAGE ON DATABASE common_db TO ROLE PUBLIC;


-- =============================================
-- STEP 3: Create and transfer Sales database
-- =============================================
-- SYSADMIN creates the database, then transfers ownership to sales_admin
CREATE DATABASE sales_database;

-- Transfer ownership of the database to sales_admin
GRANT OWNERSHIP ON DATABASE sales_database TO ROLE sales_admin;

-- IMPORTANT: Also transfer ownership of the default PUBLIC schema
-- Without this, sales_admin cannot create objects in sales_database.public
GRANT OWNERSHIP ON SCHEMA sales_database.public TO ROLE sales_admin;

-- Verify the ownership transfer
SHOW DATABASES;
-- Look at the "owner" column — sales_database should show "SALES_ADMIN"


-- =============================================
-- STEP 4: Create and transfer HR database
-- =============================================
-- Create HR database (note: hr_admin is NOT connected to SYSADMIN!)
CREATE DATABASE hr_db;

-- Transfer ownership to hr_admin
GRANT OWNERSHIP ON DATABASE hr_db TO ROLE hr_admin;
GRANT OWNERSHIP ON SCHEMA hr_db.public TO ROLE hr_admin;

-- WARNING: Since hr_admin is NOT granted to SYSADMIN, after this transfer
-- SYSADMIN can no longer see or manage hr_db! Only ACCOUNTADMIN can.
-- This demonstrates why the SYSADMIN connection best practice matters.


-- =============================================================================
-- CONCEPT GAP: OWNERSHIP vs PRIVILEGES
-- =============================================================================
-- ┌──────────────────────┬────────────────────────┬──────────────────────────┐
-- │ Concept              │ OWNERSHIP              │ PRIVILEGES               │
-- ├──────────────────────┼────────────────────────┼──────────────────────────┤
-- │ What it gives        │ Full control (DDL+DML) │ Specific actions only    │
-- │ Transfer command     │ GRANT OWNERSHIP ON ... │ GRANT <priv> ON ...     │
-- │ Revoke possible?     │ Transfer to another    │ Yes (REVOKE <priv>)     │
-- │ Multiple holders?    │ No (one owner only)    │ Yes (many grantees)     │
-- │ Includes DDL?        │ Yes (ALTER, DROP)      │ Only if explicitly given │
-- │ Includes DML?        │ Yes (all DML)          │ Only if explicitly given │
-- └──────────────────────┴────────────────────────┴──────────────────────────┘
--
-- OWNERSHIP = "This role is the admin of this object"
-- PRIVILEGE = "This role can perform specific actions on this object"
--
-- Example: SALES_ADMIN owns SALES_DATABASE:
--   - Can CREATE/ALTER/DROP tables, schemas, views
--   - Can GRANT privileges on its objects to other roles
--   - Can transfer ownership to another role
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: WAREHOUSE SIZING AND COST MANAGEMENT
-- =============================================================================
-- WAREHOUSE SIZES AND CREDITS PER HOUR:
-- ┌──────────────┬─────────────────┬────────────────────────────────────────┐
-- │ Size         │ Credits/Hour    │ Use Case                               │
-- ├──────────────┼─────────────────┼────────────────────────────────────────┤
-- │ X-Small      │ 1               │ Light queries, development            │
-- │ Small        │ 2               │ Small team, simple analytics          │
-- │ Medium       │ 4               │ Medium workloads, ETL jobs            │
-- │ Large        │ 8               │ Heavy analytics, large ETL            │
-- │ X-Large      │ 16              │ Complex queries, many concurrent users│
-- │ 2X-Large     │ 32              │ Very heavy workloads                  │
-- │ 3X-Large     │ 64              │ Enterprise-scale processing           │
-- │ 4X-Large     │ 128             │ Maximum compute power                 │
-- └──────────────┴─────────────────┴────────────────────────────────────────┘
--
-- COST CONTROL BEST PRACTICES:
--   1. Set AUTO_SUSPEND = 60-300 (seconds) to stop billing when idle
--   2. Set AUTO_RESUME = TRUE for seamless user experience
--   3. Use separate warehouses for different workloads (ETL vs analytics)
--   4. Use resource monitors to set credit limits
--   5. Start small and scale up — doubling size doubles both speed and cost
-- =============================================================================
