-- =============================================================================
-- 08_credits_consumed.sql - measure what the warehouse has cost
--
-- Run in Snowsight one block at a time: select the block, then Ctrl+Enter.
-- Needs ACCOUNTADMIN: the SNOWFLAKE.ACCOUNT_USAGE views are refused to the
-- tool role on purpose (see 03_verify_access.sql). Their rows arrive with a
-- delay of up to three hours, so step D reads the INFORMATION_SCHEMA function,
-- which has no delay but keeps only six months.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Preamble
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE NYC_TAXI_WH;

-- -----------------------------------------------------------------------------
-- Step A - the warehouse cannot burn credits while idle
-- -----------------------------------------------------------------------------

-- Expected: NYC_TAXI_WH, X-Small, auto_suspend 60, auto_resume true, and the
-- state SUSPENDED or STARTED depending on the last query. The other rows are
-- the warehouses the trial account creates by itself.
SHOW WAREHOUSES
    ->> SELECT "name", "size", "auto_suspend", "auto_resume", "state", "created_on"
        FROM $1 ORDER BY "name";

-- -----------------------------------------------------------------------------
-- Step B - credits per warehouse since the account was created
-- -----------------------------------------------------------------------------

-- Expected: NYC_TAXI_WH first, with the compute and cloud-services split, and
-- the first and last hour it ran.
SELECT
    warehouse_name,
    ROUND(SUM(credits_used), 3)                AS credits,
    ROUND(SUM(credits_used_compute), 3)        AS credits_compute,
    ROUND(SUM(credits_used_cloud_services), 3) AS credits_cloud_services,
    MIN(start_time)                            AS first_hour,
    MAX(end_time)                              AS last_hour
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY warehouse_name
ORDER BY credits DESC;

-- -----------------------------------------------------------------------------
-- Step C - credits per day for the pipeline warehouse
-- -----------------------------------------------------------------------------

-- Expected: one row per day the pipeline ran. The heaviest days are those of
-- the loads and of the three-month backfill.
SELECT
    start_time::date            AS day,
    ROUND(SUM(credits_used), 3) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'NYC_TAXI_WH'
GROUP BY day
ORDER BY day;

-- -----------------------------------------------------------------------------
-- Step D - the same, without the delay (last seven days, hour by hour)
-- -----------------------------------------------------------------------------

-- Expected: the hours of today already appear here, whereas step C may still
-- lag behind.
SELECT
    start_time,
    ROUND(credits_used, 3)         AS credits,
    ROUND(credits_used_compute, 3) AS credits_compute
FROM TABLE(NYC_TAXI.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD(DAY, -7, CURRENT_DATE()),
    DATE_RANGE_END => DATEADD(DAY, 1, CURRENT_DATE()),
    WAREHOUSE_NAME => 'NYC_TAXI_WH'
))
ORDER BY start_time;

-- -----------------------------------------------------------------------------
-- Step E - the whole account, every service, per day
-- -----------------------------------------------------------------------------

-- Expected: WAREHOUSE_METERING rows for NYC_TAXI_WH and, if any, rows for the
-- services that run without a warehouse (cloud services, serverless tasks).
SELECT
    usage_date,
    service_type,
    ROUND(SUM(credits_used), 3) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
GROUP BY usage_date, service_type
ORDER BY usage_date, service_type;
