-- =============================================================================
-- 06_raw_parquet_logical_types.sql - fix the Parquet format, empty what it fed
--
-- Run in Snowsight one block at a time, as the tool role: it owns the format
-- and the table. One-off repair of an installation made before 05 declared
-- USE_LOGICAL_TYPE; a fresh installation does not need it.
-- Why: without USE_LOGICAL_TYPE, the TIMESTAMP(MICROS) columns of the TLC
-- files were read as integers of microseconds and stored as if they were
-- seconds, millions of years away. Every loaded month is wrong the same way.
-- =============================================================================
USE ROLE TRANSFORMER;
USE WAREHOUSE NYC_TAXI_WH;

-- -----------------------------------------------------------------------------
-- 1. The format - honour the logical types from now on
-- -----------------------------------------------------------------------------
ALTER FILE FORMAT NYC_TAXI.RAW.PARQUET_FF SET USE_LOGICAL_TYPE = TRUE;

-- Expected: one row, USE_LOGICAL_TYPE true.
DESCRIBE FILE FORMAT NYC_TAXI.RAW.PARQUET_FF
    ->> SELECT "property", "property_value" FROM $1
        WHERE "property" = 'USE_LOGICAL_TYPE';

-- Expected: 5 rows with pickup and dropoff timestamps in January 2025, read
-- straight from the staged file. The fix is proven before any reload.
SELECT
    $1:tpep_pickup_datetime::TIMESTAMP_NTZ AS tpep_pickup_datetime,
    $1:tpep_dropoff_datetime::TIMESTAMP_NTZ AS tpep_dropoff_datetime
FROM @NYC_TAXI.RAW.TLC_STAGE/yellow_tripdata_2025-01.parquet
    (FILE_FORMAT => 'NYC_TAXI.RAW.PARQUET_FF')
LIMIT 5;

-- -----------------------------------------------------------------------------
-- 2. The table - drop the wrong rows and the memory of the files
-- TRUNCATE, not DELETE: it also clears the load metadata, so COPY INTO will
-- accept the same files again. The loader's own guard reads _source_file,
-- which is empty too. The files stay in the stage: the reload skips the PUT.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE NYC_TAXI.RAW.YELLOW_TRIPDATA;

-- Expected: 0.
SELECT COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPDATA;

-- Next: reload the three months through Airflow (backfill, reprocess
-- completed), then step C of 04_verify_raw_contract.sql must be all TRUE.
