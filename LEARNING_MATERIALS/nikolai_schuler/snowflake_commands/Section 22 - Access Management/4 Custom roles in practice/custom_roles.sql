-- =============================================================================
-- SECTION 22.4: CUSTOM ROLES IN PRACTICE
-- =============================================================================
--
-- OBJECTIVE:
--   See how custom roles (SALES_ADMIN, SALES_USERS) work in practice —
--   creating tables, granting granular privileges, and validating access.
--
-- WHAT YOU WILL LEARN:
--   1. How custom admin roles create and manage objects
--   2. The three-level grant pattern (database → schema → table)
--   3. Granular privilege granting (SELECT, DELETE, INSERT, etc.)
--   4. The difference between OWNERSHIP and individual privileges
--
-- THREE-LEVEL GRANT PATTERN (CRITICAL INTERVIEW TOPIC):
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  To grant SELECT on a table, you need ALL THREE:                      │
-- │                                                                        │
-- │  1. GRANT USAGE ON DATABASE sales_database TO ROLE sales_users;       │
-- │  2. GRANT USAGE ON SCHEMA  sales_database.public TO ROLE sales_users; │
-- │  3. GRANT SELECT ON TABLE  sales_database.public.customers            │
-- │         TO ROLE sales_users;                                           │
-- │                                                                        │
-- │  Missing ANY level = "Insufficient privileges" error!                 │
-- │  This is the #1 RBAC pitfall in Snowflake interviews.                 │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- PRIVILEGE TYPES ON TABLES:
-- ┌──────────────────┬────────────────────────────────────────────────────┐
-- │ Privilege        │ What It Allows                                     │
-- ├──────────────────┼────────────────────────────────────────────────────┤
-- │ SELECT           │ Read data (query rows)                            │
-- │ INSERT           │ Add new rows                                      │
-- │ UPDATE           │ Modify existing rows                              │
-- │ DELETE           │ Remove rows                                       │
-- │ TRUNCATE         │ Remove all rows (faster than DELETE)              │
-- │ REFERENCES       │ Create foreign key constraints                    │
-- │ OWNERSHIP        │ Full control (all above + DDL like ALTER, DROP)   │
-- └──────────────────┴────────────────────────────────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - Privileges are NEVER implicit — every level must be explicitly granted
--   - USAGE on database + USAGE on schema are prerequisites for table access
--   - DROP TABLE requires OWNERSHIP, not just DELETE
--   - GRANT is additive — granting DELETE doesn't revoke SELECT
--   - Custom admin roles manage privileges for their department's users
-- =============================================================================


-- =============================================
-- STEP 1: Switch to the custom admin role
-- =============================================
USE ROLE SALES_ADMIN;
USE SALES_DATABASE;


-- =============================================
-- STEP 2: Create a table as SALES_ADMIN
-- =============================================
-- SALES_ADMIN owns SALES_DATABASE, so it can create tables
CREATE OR REPLACE TABLE customers (
    id          NUMBER,
    full_name   VARCHAR,
    email       VARCHAR,
    phone       VARCHAR,
    spent       NUMBER,
    create_date DATE DEFAULT CURRENT_DATE
);

-- Insert sample data
INSERT INTO customers (id, full_name, email, phone, spent)
VALUES
    (1, 'Lewiss MacDwyer',   'lmacdwyer0@un.org',           '262-665-9168', 140),
    (2, 'Ty Pettingall',     'tpettingall1@mayoclinic.com',  '734-987-7120', 254),
    (3, 'Marlee Spadazzi',   'mspadazzi2@txnews.com',        '867-946-3659', 120),
    (4, 'Heywood Tearney',   'htearney3@patch.com',          '563-853-8192', 1230),
    (5, 'Odilia Seti',       'oseti4@globo.com',             '730-451-8637', 143),
    (6, 'Meggie Washtell',   'mwashtell5@rediff.com',        '568-896-6138', 600);

SHOW TABLES;

-- SALES_ADMIN can query its own table
SELECT * FROM CUSTOMERS;


-- =============================================
-- STEP 3: Test SALES_USERS access (before grants)
-- =============================================
USE ROLE SALES_USERS;
-- This will FAIL — no privileges granted yet:
-- SELECT * FROM CUSTOMERS;
-- Error: "Object 'CUSTOMERS' does not exist or not authorized"


-- =============================================
-- STEP 4: Grant the three-level access pattern
-- =============================================
USE ROLE SALES_ADMIN;

-- Level 1: Database usage
GRANT USAGE ON DATABASE SALES_DATABASE TO ROLE SALES_USERS;

-- Level 2: Schema usage
GRANT USAGE ON SCHEMA SALES_DATABASE.PUBLIC TO ROLE SALES_USERS;

-- Level 3: Table privilege (SELECT only — read access)
GRANT SELECT ON TABLE SALES_DATABASE.PUBLIC.CUSTOMERS TO ROLE SALES_USERS;


-- =============================================
-- STEP 5: Validate SALES_USERS privileges
-- =============================================
USE ROLE SALES_USERS;

-- SELECT works (granted above)
SELECT * FROM CUSTOMERS;

-- DROP TABLE fails (requires OWNERSHIP, not just SELECT)
-- DROP TABLE CUSTOMERS;
-- Error: "Insufficient privileges to operate on table 'CUSTOMERS'"

-- DELETE fails (not granted yet)
-- DELETE FROM CUSTOMERS;
-- Error: "Insufficient privileges"

-- Check what tables are visible
SHOW TABLES;


-- =============================================
-- STEP 6: Grant additional privileges
-- =============================================
USE ROLE SALES_ADMIN;

-- Grant DELETE privilege (allows removing individual rows)
GRANT DELETE ON TABLE SALES_DATABASE.PUBLIC.CUSTOMERS TO ROLE SALES_USERS;

-- Now SALES_USERS can delete rows but still CANNOT:
--   - DROP TABLE (requires OWNERSHIP)
--   - ALTER TABLE (requires OWNERSHIP)
--   - INSERT rows (not granted)
--   - UPDATE rows (not granted)

USE ROLE SALES_USERS;
-- DELETE now works:
-- DELETE FROM CUSTOMERS WHERE id = 1;


-- =============================================================================
-- CONCEPT GAP: PRIVILEGE ESCALATION AWARENESS
-- =============================================================================
-- Q: "Can SALES_USERS grant SELECT to another role?"
-- A: NO. Only the OWNER (SALES_ADMIN) or a role with MANAGE GRANTS
--    can grant privileges. SALES_USERS has received SELECT, not the
--    ability to share it.
--
-- Q: "Can SALES_USERS create tables in SALES_DATABASE?"
-- A: NO. Creating tables requires OWNERSHIP of the schema or
--    CREATE TABLE privilege on the schema, neither of which was granted.
--
-- Q: "What if SALES_ADMIN grants ALL PRIVILEGES?"
-- A: GRANT ALL PRIVILEGES ON TABLE ... TO ROLE SALES_USERS;
--    This grants SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES
--    but NOT OWNERSHIP. The user still cannot ALTER or DROP the table.
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: FUTURE GRANTS (PRODUCTION PATTERN)
-- =============================================================================
-- Problem: When SALES_ADMIN creates NEW tables, SALES_USERS has no access
--          until explicit GRANT commands are run for each new table.
--
-- Solution: FUTURE GRANTS automatically apply privileges to new objects
--
--   -- Grant SELECT on all FUTURE tables in the schema
--   GRANT SELECT ON FUTURE TABLES IN SCHEMA sales_database.public
--       TO ROLE SALES_USERS;
--
--   -- Now any new table created in this schema automatically gets
--   -- SELECT granted to SALES_USERS — no manual grant needed!
--
-- IMPORTANT NOTES:
--   - Future grants only apply to NEW objects, not existing ones
--   - Use regular GRANT for existing objects + FUTURE GRANT for new ones
--   - Works for TABLES, VIEWS, STAGES, FUNCTIONS, PROCEDURES, etc.
--   - SHOW FUTURE GRANTS lists all active future grants
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: REVOKING PRIVILEGES
-- =============================================================================
-- REVOKE is the opposite of GRANT:
--
--   REVOKE SELECT ON TABLE customers FROM ROLE sales_users;
--   REVOKE USAGE ON SCHEMA sales_database.public FROM ROLE sales_users;
--   REVOKE USAGE ON DATABASE sales_database FROM ROLE sales_users;
--
-- IMPORTANT: Revoking USAGE on the database effectively blocks ALL access
-- to everything inside it, even if table-level grants still exist.
-- The three-level grant pattern works in reverse too!
-- =============================================================================
