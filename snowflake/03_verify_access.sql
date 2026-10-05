-- =============================================================================
-- 03_verify_access.sql - prove what the tool role can and cannot do
--
-- Run in Snowsight one block at a time: select the block, then Ctrl+Enter.
-- Never "Run All": the statements of step C are all expected to fail.
-- Run the preamble again before each step if the worksheet stayed idle.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Preamble - become the tool role, and only the tool role
-- -----------------------------------------------------------------------------
USE ROLE TRANSFORMER;

-- A Snowsight session also activates the other roles of the signed-in user as
-- secondary roles. They authorize everything except CREATE statements, so some
-- refusals of step C would wrongly succeed for an administrator.
USE SECONDARY ROLES NONE;

USE WAREHOUSE NYC_TAXI_WH;

-- Guard: must return TRANSFORMER and no secondary role.
SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES();

-- -----------------------------------------------------------------------------
-- Step A - read what was granted
-- -----------------------------------------------------------------------------

-- Expected: 15 rows (1 warehouse, 1 database, 4 on RAW, 3 on each other schema).
SHOW GRANTS TO ROLE TRANSFORMER;

-- Expected: 2 rows, the role SYSADMIN and the user AIRFLOW_SVC.
SHOW GRANTS OF ROLE TRANSFORMER;

-- Expected: size X-Small, auto_suspend 60, auto_resume true.
SHOW WAREHOUSES LIKE 'NYC_TAXI_WH';

-- -----------------------------------------------------------------------------
-- Step B - every granted privilege works (each object is dropped right away)
-- Expected: 18 successful statements, nothing left behind.
-- -----------------------------------------------------------------------------

-- RAW: table, stage, file format
CREATE TABLE NYC_TAXI.RAW.ZZ_ACCESS_TEST (id INTEGER);
DROP TABLE NYC_TAXI.RAW.ZZ_ACCESS_TEST;
CREATE STAGE NYC_TAXI.RAW.ZZ_ACCESS_TEST;
DROP STAGE NYC_TAXI.RAW.ZZ_ACCESS_TEST;
CREATE FILE FORMAT NYC_TAXI.RAW.ZZ_ACCESS_TEST TYPE = PARQUET;
DROP FILE FORMAT NYC_TAXI.RAW.ZZ_ACCESS_TEST;

-- STAGING: table, view
CREATE TABLE NYC_TAXI.STAGING.ZZ_ACCESS_TEST (id INTEGER);
DROP TABLE NYC_TAXI.STAGING.ZZ_ACCESS_TEST;
CREATE VIEW NYC_TAXI.STAGING.ZZ_ACCESS_TEST AS SELECT 1 AS id;
DROP VIEW NYC_TAXI.STAGING.ZZ_ACCESS_TEST;

-- INTERMEDIATE: table, view
CREATE TABLE NYC_TAXI.INTERMEDIATE.ZZ_ACCESS_TEST (id INTEGER);
DROP TABLE NYC_TAXI.INTERMEDIATE.ZZ_ACCESS_TEST;
CREATE VIEW NYC_TAXI.INTERMEDIATE.ZZ_ACCESS_TEST AS SELECT 1 AS id;
DROP VIEW NYC_TAXI.INTERMEDIATE.ZZ_ACCESS_TEST;

-- MARTS: table, view
CREATE TABLE NYC_TAXI.MARTS.ZZ_ACCESS_TEST (id INTEGER);
DROP TABLE NYC_TAXI.MARTS.ZZ_ACCESS_TEST;
CREATE VIEW NYC_TAXI.MARTS.ZZ_ACCESS_TEST AS SELECT 1 AS id;
DROP VIEW NYC_TAXI.MARTS.ZZ_ACCESS_TEST;

-- Nothing left behind. Expected: no row in any of the three results.
SHOW OBJECTS LIKE 'ZZ%' IN DATABASE NYC_TAXI;
SHOW STAGES LIKE 'ZZ%' IN DATABASE NYC_TAXI;
SHOW FILE FORMATS LIKE 'ZZ%' IN DATABASE NYC_TAXI;

-- -----------------------------------------------------------------------------
-- Step C - everything else is refused: run ONE statement at a time
-- Check the guard of the preamble first: no secondary role must be active.
-- Expected: 8 refusals.
-- -----------------------------------------------------------------------------

-- Inside the schemas: no CREATE VIEW on RAW, no CREATE STAGE on MARTS
CREATE VIEW NYC_TAXI.RAW.ZZ_ACCESS_TEST AS SELECT 1 AS id;
CREATE STAGE NYC_TAXI.MARTS.ZZ_ACCESS_TEST;

-- Outside the schemas: no CREATE SCHEMA on the database, no CREATE DATABASE
CREATE SCHEMA NYC_TAXI.ZZ_ACCESS_TEST;
CREATE DATABASE ZZ_ACCESS_TEST;

-- Compute: no MODIFY (resize), no OPERATE (suspend)
ALTER WAREHOUSE NYC_TAXI_WH SET WAREHOUSE_SIZE = 'SMALL';
ALTER WAREHOUSE NYC_TAXI_WH SUSPEND;

-- Account: no user creation, no access to the credit consumption
CREATE USER ZZ_ACCESS_TEST;
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1;

-- Safety net: the warehouse must still be X-Small.
SHOW WAREHOUSES LIKE 'NYC_TAXI_WH';

-- -----------------------------------------------------------------------------
-- Step D - inventory: what the role sees without any grant of ours
-- Every role inherits PUBLIC, so the lists are not limited to our own objects.
-- -----------------------------------------------------------------------------
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE NYC_TAXI;
SHOW WAREHOUSES;

-- Where the extra visibility comes from.
SHOW GRANTS TO ROLE PUBLIC;
