-- =============================================================================
-- 07_verify_data_quality.sql - count the anomalous trips of a month by hand
-- and compare with MARTS.MART_DATA_QUALITY
--
-- Run in Snowsight one block at a time: select the block, then Ctrl+Enter.
-- The recount reads the STAGING view, so it never goes through
-- INT_TRIPS__FLAGGED: same rules, other path. The rules are those of
-- airflow/include/sql/intermediate/int_trips__flagged.sql, the thresholds
-- those of the DAG params. The expected values are those of January 2025.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Preamble - become the tool role, and only the tool role
-- -----------------------------------------------------------------------------
USE ROLE TRANSFORMER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE NYC_TAXI_WH;

-- Guard: must return TRANSFORMER and no secondary role.
SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES();

-- The month to check, and the thresholds of the DAG params.
SET month = '2025-01-01';
SET max_trip_distance_miles = 100;
SET max_trip_duration_min = 180;

-- -----------------------------------------------------------------------------
-- Step A - each rule counted on its own
-- A trip that breaks several rules is counted once per rule here, whereas the
-- mart gives it a single reason, the first of the cascade. The figures of this
-- step are therefore greater than or equal to those of the mart.
-- -----------------------------------------------------------------------------

-- Expected: 1 row, 3475226, 0, 2051, 1377, 22, 91055, 144998, 0, 223889.
SELECT
    COUNT(*)                                                      AS nb_rows,
    COUNT_IF(pickup_at IS NULL OR dropoff_at IS NULL)             AS timestamp_null,
    COUNT_IF(dropoff_at <= pickup_at)                             AS duration_non_positive,
    COUNT_IF(DATEDIFF('second', pickup_at, dropoff_at)
        > $max_trip_duration_min * 60)                            AS duration_too_long,
    COUNT_IF(DATE_TRUNC('month', pickup_at) <> source_file_month) AS pickup_outside_file_month,
    COUNT_IF(trip_distance_miles <= 0
        OR trip_distance_miles > $max_trip_distance_miles)        AS distance_out_of_range,
    COUNT_IF(fare_amount < 0 OR total_amount <= 0)                AS amount_non_positive,
    COUNT_IF(pickup_zone_key IS NULL OR dropoff_zone_key IS NULL) AS zone_null,
    COUNT_IF(pickup_at IS NULL OR dropoff_at IS NULL
        OR dropoff_at <= pickup_at
        OR DATEDIFF('second', pickup_at, dropoff_at) > $max_trip_duration_min * 60
        OR DATE_TRUNC('month', pickup_at) <> source_file_month
        OR trip_distance_miles <= 0 OR trip_distance_miles > $max_trip_distance_miles
        OR fare_amount < 0 OR total_amount <= 0
        OR pickup_zone_key IS NULL OR dropoff_zone_key IS NULL)   AS rejected_any
FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS
WHERE source_file_month = $month;

-- -----------------------------------------------------------------------------
-- Step B - how many rules each trip breaks
-- COALESCE: a comparison on a NULL timestamp is NULL, which would poison the sum.
-- -----------------------------------------------------------------------------

-- Expected: 4 rows, 0 -> 3251337, 1 -> 208313, 2 -> 15538, 3 -> 38.
-- Rows 1 to 3 add up to rejected_any (223889). The per-rule counts of step A
-- add up to 239503, that is 15538 + 2 * 38 more: the trips counted twice or
-- three times above.
SELECT nb_rules_failed, COUNT(*) AS nb_trips
FROM (
    SELECT
        COALESCE(pickup_at IS NULL OR dropoff_at IS NULL, FALSE)::int
        + COALESCE(dropoff_at <= pickup_at, FALSE)::int
        + COALESCE(DATEDIFF('second', pickup_at, dropoff_at)
            > $max_trip_duration_min * 60, FALSE)::int
        + COALESCE(DATE_TRUNC('month', pickup_at) <> source_file_month, FALSE)::int
        + COALESCE(trip_distance_miles <= 0
            OR trip_distance_miles > $max_trip_distance_miles, FALSE)::int
        + COALESCE(fare_amount < 0 OR total_amount <= 0, FALSE)::int
        + COALESCE(pickup_zone_key IS NULL OR dropoff_zone_key IS NULL, FALSE)::int
            AS nb_rules_failed
    FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS
    WHERE source_file_month = $month
)
GROUP BY nb_rules_failed
ORDER BY nb_rules_failed;

-- -----------------------------------------------------------------------------
-- Step C - what the mart says for the same month
-- -----------------------------------------------------------------------------

-- Expected: 6 rows. valid 3251337 (93.558 %), amount_non_positive 130112,
-- distance_out_of_range 90327, duration_non_positive 2051,
-- duration_too_long 1377, pickup_outside_file_month 22.
-- amount_non_positive and distance_out_of_range are below step A: the trips
-- that also break an earlier rule are filed under that earlier rule.
SELECT status, nb_rows, nb_rows_total, pct_of_file
FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
WHERE source_file_month = $month
ORDER BY nb_rows DESC;

-- -----------------------------------------------------------------------------
-- Step D - the cascade recounted from STAGING matches the mart exactly
-- -----------------------------------------------------------------------------

-- Expected: no row. Each row returned is a status whose count differs between
-- the recount and the mart, or that exists on one side only.
WITH recount AS (
    SELECT
        COALESCE(
            CASE
                WHEN pickup_at IS NULL OR dropoff_at IS NULL
                    THEN 'timestamp_null'
                WHEN dropoff_at <= pickup_at
                    THEN 'duration_non_positive'
                WHEN DATEDIFF('second', pickup_at, dropoff_at) > $max_trip_duration_min * 60
                    THEN 'duration_too_long'
                WHEN DATE_TRUNC('month', pickup_at) <> source_file_month
                    THEN 'pickup_outside_file_month'
                WHEN trip_distance_miles <= 0 OR trip_distance_miles > $max_trip_distance_miles
                    THEN 'distance_out_of_range'
                WHEN fare_amount < 0 OR total_amount <= 0
                    THEN 'amount_non_positive'
                WHEN pickup_zone_key IS NULL OR dropoff_zone_key IS NULL
                    THEN 'zone_null'
            END,
            'valid'
        ) AS status,
        COUNT(*) AS nb_rows
    FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS
    WHERE source_file_month = $month
    GROUP BY 1
),
mart AS (
    SELECT status, nb_rows
    FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
    WHERE source_file_month = $month
)
SELECT
    COALESCE(recount.status, mart.status) AS status,
    recount.nb_rows AS recounted,
    mart.nb_rows AS in_mart
FROM recount
FULL OUTER JOIN mart ON mart.status = recount.status
WHERE recount.nb_rows IS DISTINCT FROM mart.nb_rows
ORDER BY 1;
