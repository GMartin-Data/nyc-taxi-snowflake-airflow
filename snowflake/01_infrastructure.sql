-- =============================================================================
-- 01_infrastructure.sql - NYC Taxi warehouse: role, compute, database, grants
--
-- Run in Snowsight with "Run All", signed in as an account administrator.
-- Replayable: every CREATE uses IF NOT EXISTS, every GRANT and ALTER is
-- idempotent.
-- The order follows the dependencies:
--   role -> warehouse -> database -> schemas -> grants -> service user
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Tool role (USERADMIN is the system role that creates roles and users)
-- -----------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'Tool role: loads RAW, builds STAGING, INTERMEDIATE and MARTS';

-- Attach the custom role to the system hierarchy: SYSADMIN inherits it, so
-- administrators can see and manage every object the tools will create.
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;

-- -----------------------------------------------------------------------------
-- 2. Compute and containers (SYSADMIN creates them, and therefore owns them)
-- -----------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60            -- seconds of inactivity before suspension
    AUTO_RESUME = TRUE           -- wakes up by itself when a query arrives
    INITIALLY_SUSPENDED = TRUE   -- no credit spent at creation
    COMMENT = 'Single XS warehouse of the NYC Taxi pipeline';

CREATE DATABASE IF NOT EXISTS NYC_TAXI
    COMMENT = 'NYC Yellow Taxi medallion pipeline';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI.RAW
    COMMENT = 'Faithful copy of the source files, plus load metadata';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.STAGING
    COMMENT = 'Renamed and typed columns';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.INTERMEDIATE
    COMMENT = 'Flagged trips, then valid trips enriched';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.MARTS
    COMMENT = 'Fact table, dimensions and analysis tables';

-- -----------------------------------------------------------------------------
-- 3. Privileges of the tool role (least privilege: one line per actual need)
-- -----------------------------------------------------------------------------

-- Compute: run queries. No OPERATE: AUTO_RESUME starts the warehouse on demand.
GRANT USAGE ON WAREHOUSE NYC_TAXI_WH TO ROLE TRANSFORMER;

-- Access chain: the database first, then each schema.
GRANT USAGE ON DATABASE NYC_TAXI TO ROLE TRANSFORMER;

-- RAW: loading needs tables, a stage and file formats.
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
    ON SCHEMA NYC_TAXI.RAW TO ROLE TRANSFORMER;

-- Transformation layers: tables and views.
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.STAGING      TO ROLE TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.MARTS        TO ROLE TRANSFORMER;

-- No grant on any table, view or stage: TRANSFORMER creates them itself and
-- owns them, which already gives it every right on its own objects.

-- -----------------------------------------------------------------------------
-- 4. Service user (identity shared by the Python script and Airflow)
-- -----------------------------------------------------------------------------
USE ROLE USERADMIN;

-- TYPE = SERVICE: no password, no web interface, key-pair authentication only.
-- The public key is registered separately: see 02_service_user_key.sql.
CREATE USER IF NOT EXISTS AIRFLOW_SVC
    TYPE = SERVICE
    DEFAULT_ROLE = TRANSFORMER
    DEFAULT_WAREHOUSE = NYC_TAXI_WH
    DEFAULT_NAMESPACE = NYC_TAXI.RAW
    COMMENT = 'Service account of the loading and transformation tools';

-- DEFAULT_ROLE only selects the role activated at login: this GRANT gives it.
GRANT ROLE TRANSFORMER TO USER AIRFLOW_SVC;

-- Session time zone of the tools. COPY INTO converts METADATA$START_SCAN_TIME
-- to TIMESTAMP_NTZ through it; the Snowflake default is America/Los_Angeles,
-- which _loaded_at would then carry without saying so.
ALTER USER AIRFLOW_SVC SET TIMEZONE = 'UTC';

-- Check: the TIMEZONE row must show value UTC at level USER, not the account
-- default. This is the single setting that every client of the user inherits.
SHOW PARAMETERS LIKE 'TIMEZONE' IN USER AIRFLOW_SVC;
