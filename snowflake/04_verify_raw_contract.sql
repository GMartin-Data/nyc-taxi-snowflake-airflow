-- =============================================================================
-- 04_verify_raw_contract.sql - prove that the RAW layer honours CONTRAT_RAW.md
--
-- Run in Snowsight one block at a time: select the block, then Ctrl+Enter.
-- Each "Expected" line states the passing result. Until the RAW objects exist
-- and the files are loaded, the steps return other rows or fail.
-- The expected counts were measured in the source files with DuckDB.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Preamble - become the tool role, and only the tool role
-- -----------------------------------------------------------------------------
USE ROLE TRANSFORMER;

-- The secondary roles of an administrator would also show objects that the
-- tools cannot see: a table created by the wrong role would pass unnoticed.
USE SECONDARY ROLES NONE;

USE WAREHOUSE NYC_TAXI_WH;

-- Guard: must return TRANSFORMER and no secondary role.
SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES();

-- -----------------------------------------------------------------------------
-- Step A - the objects exist and belong to the tool role
-- -----------------------------------------------------------------------------

-- Expected: 2 rows, TAXI_ZONE_LOOKUP and YELLOW_TRIPDATA, owner TRANSFORMER.
SHOW TABLES IN SCHEMA NYC_TAXI.RAW
    ->> SELECT "name", "owner", "rows" FROM $1 ORDER BY "name";

-- Expected: 1 row, TLC_STAGE, type INTERNAL, owner TRANSFORMER.
SHOW STAGES IN SCHEMA NYC_TAXI.RAW
    ->> SELECT "name", "type", "owner" FROM $1;

-- Expected: 2 rows, CSV_FF and PARQUET_FF, owner TRANSFORMER.
SHOW FILE FORMATS IN SCHEMA NYC_TAXI.RAW
    ->> SELECT "name", "type", "owner" FROM $1 ORDER BY "name";

-- -----------------------------------------------------------------------------
-- Step B - the columns are the ones of the contract
-- INFORMATION_SCHEMA reports VARCHAR as TEXT.
-- -----------------------------------------------------------------------------

-- Expected: no row. Each row returned is a column that is missing, unexpected
-- or of another type.
WITH expected (table_name, column_name, data_type) AS (
    SELECT * FROM VALUES
        ('YELLOW_TRIPDATA', 'VENDORID', 'NUMBER'),
        ('YELLOW_TRIPDATA', 'TPEP_PICKUP_DATETIME', 'TIMESTAMP_NTZ'),
        ('YELLOW_TRIPDATA', 'TPEP_DROPOFF_DATETIME', 'TIMESTAMP_NTZ'),
        ('YELLOW_TRIPDATA', 'PASSENGER_COUNT', 'NUMBER'),
        ('YELLOW_TRIPDATA', 'TRIP_DISTANCE', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'RATECODEID', 'NUMBER'),
        ('YELLOW_TRIPDATA', 'STORE_AND_FWD_FLAG', 'TEXT'),
        ('YELLOW_TRIPDATA', 'PULOCATIONID', 'NUMBER'),
        ('YELLOW_TRIPDATA', 'DOLOCATIONID', 'NUMBER'),
        ('YELLOW_TRIPDATA', 'PAYMENT_TYPE', 'NUMBER'),
        ('YELLOW_TRIPDATA', 'FARE_AMOUNT', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'EXTRA', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'MTA_TAX', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'TIP_AMOUNT', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'TOLLS_AMOUNT', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'IMPROVEMENT_SURCHARGE', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'TOTAL_AMOUNT', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'CONGESTION_SURCHARGE', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'AIRPORT_FEE', 'FLOAT'),
        ('YELLOW_TRIPDATA', 'CBD_CONGESTION_FEE', 'FLOAT'),
        ('YELLOW_TRIPDATA', '_SOURCE_FILE', 'TEXT'),
        ('YELLOW_TRIPDATA', '_LOADED_AT', 'TIMESTAMP_NTZ'),
        ('TAXI_ZONE_LOOKUP', 'LOCATIONID', 'NUMBER'),
        ('TAXI_ZONE_LOOKUP', 'BOROUGH', 'TEXT'),
        ('TAXI_ZONE_LOOKUP', 'ZONE', 'TEXT'),
        ('TAXI_ZONE_LOOKUP', 'SERVICE_ZONE', 'TEXT'),
        ('TAXI_ZONE_LOOKUP', '_SOURCE_FILE', 'TEXT'),
        ('TAXI_ZONE_LOOKUP', '_LOADED_AT', 'TIMESTAMP_NTZ')
),
actual AS (
    SELECT table_name, column_name, data_type
    FROM NYC_TAXI.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'RAW'
)
SELECT
    COALESCE(expected.table_name, actual.table_name) AS table_name,
    COALESCE(expected.column_name, actual.column_name) AS column_name,
    expected.data_type AS expected_type,
    actual.data_type AS actual_type
FROM expected
FULL OUTER JOIN actual
    ON expected.table_name = actual.table_name
    AND expected.column_name = actual.column_name
WHERE expected.data_type IS NULL
    OR actual.data_type IS NULL
    OR expected.data_type <> actual.data_type
ORDER BY 1, 2;

-- -----------------------------------------------------------------------------
-- Step C - January 2025 is loaded, faithfully
-- -----------------------------------------------------------------------------

-- Expected: 1 row, 3475226 then TRUE in the four *_ok columns.
-- A file staged in a folder would not match the filter: _source_file must be
-- the file name alone. Amounts rounded to integers would give 0 decimal rows.
SELECT
    COUNT(*) AS row_count,
    COUNT(*) = 3475226 AS row_count_ok,
    COUNT(_loaded_at) = 3475226 AS loaded_at_ok,
    COUNT_IF(fare_amount <> ROUND(fare_amount)) = 3069449 AS fare_decimals_ok,
    COUNT_IF(total_amount <> ROUND(total_amount)) = 3312453 AS total_decimals_ok
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
WHERE _source_file = 'yellow_tripdata_2025-01.parquet';

-- Expected: 1 row per loaded month, each named yellow_tripdata_YYYY-MM.parquet.
SELECT
    _source_file,
    COUNT(*) AS row_count,
    MIN(_loaded_at) AS first_loaded_at
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
GROUP BY _source_file
ORDER BY _source_file;

-- -----------------------------------------------------------------------------
-- Step D - loading January a second time adds nothing
-- Run the load again, then step C again: still 3475226 rows.
-- -----------------------------------------------------------------------------

-- Expected: exactly 1 row per file, status Loaded. The second load leaves no
-- row here because it processed no file.
SELECT file_name, status, row_count, last_load_time
FROM TABLE(NYC_TAXI.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'NYC_TAXI.RAW.YELLOW_TRIPDATA',
    START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time;

-- -----------------------------------------------------------------------------
-- Step E - the 265 taxi zones are loaded
-- -----------------------------------------------------------------------------

-- Expected: 1 row, 265 then TRUE in the four *_ok columns.
SELECT
    COUNT(*) AS row_count,
    COUNT(*) = 265 AS row_count_ok,
    COUNT(DISTINCT locationid) = 265 AS locationid_unique_ok,
    COUNT(borough) = 265 AND COUNT(zone) = 265 AND COUNT(service_zone) = 265
        AS text_columns_ok,
    COUNT_IF(_source_file = 'taxi_zone_lookup.csv') = 265
        AND COUNT(_loaded_at) = 265 AS technical_columns_ok
FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP;
