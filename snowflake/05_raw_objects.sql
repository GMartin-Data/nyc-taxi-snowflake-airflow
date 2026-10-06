-- =============================================================================
-- 05_raw_objects.sql - RAW layer: file formats, stage and the two tables
--
-- Run in Snowsight with "Run All", as the tool role: the role that creates an
-- object owns it, and the tools must own what they load into.
-- Replayable: every CREATE uses IF NOT EXISTS. Never CREATE OR REPLACE a
-- table here: it would drop the rows and the memory of the files already
-- loaded into it.
-- Names, columns and types follow CONTRAT_RAW.md; 04_verify_raw_contract.sql
-- checks them.
-- =============================================================================
USE ROLE TRANSFORMER;

-- -----------------------------------------------------------------------------
-- 1. File formats - how to read each kind of file
-- -----------------------------------------------------------------------------

-- A Parquet file carries its own column names and types: nothing to declare.
CREATE FILE FORMAT IF NOT EXISTS NYC_TAXI.RAW.PARQUET_FF
    TYPE = PARQUET
    COMMENT = 'Monthly TLC trip files';

-- A CSV file describes nothing by itself: header, quotes and column count.
CREATE FILE FORMAT IF NOT EXISTS NYC_TAXI.RAW.CSV_FF
    TYPE = CSV
    PARSE_HEADER = TRUE                       -- column names come from line 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'        -- "Newark Airport"
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE    -- 4 columns in the file, 6 in the table
    COMMENT = 'TLC taxi zone lookup';

-- -----------------------------------------------------------------------------
-- 2. Stage - where the files land before COPY INTO
-- Files go to its root: _source_file receives the path inside the stage, and
-- the downstream SQL expects the bare file name.
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS NYC_TAXI.RAW.TLC_STAGE
    COMMENT = 'Landing zone of the TLC files: monthly trips and zone lookup';

-- -----------------------------------------------------------------------------
-- 3. Tables - wide types, declared rather than inferred from one file
-- NUMBER for integers; FLOAT for amounts and distances, because NUMBER
-- without scale would round 12.75 to 13.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.YELLOW_TRIPDATA (
    vendorid                NUMBER,
    tpep_pickup_datetime    TIMESTAMP_NTZ,
    tpep_dropoff_datetime   TIMESTAMP_NTZ,
    passenger_count         NUMBER,
    trip_distance           FLOAT,
    ratecodeid              NUMBER,
    store_and_fwd_flag      VARCHAR,
    pulocationid            NUMBER,
    dolocationid            NUMBER,
    payment_type            NUMBER,
    fare_amount             FLOAT,
    extra                   FLOAT,
    mta_tax                 FLOAT,
    tip_amount              FLOAT,
    tolls_amount            FLOAT,
    improvement_surcharge   FLOAT,
    total_amount            FLOAT,
    congestion_surcharge    FLOAT,
    airport_fee             FLOAT,
    cbd_congestion_fee      FLOAT,
    -- Absent from the files, filled by COPY INTO
    _source_file            VARCHAR,
    _loaded_at              TIMESTAMP_NTZ
)
COMMENT = 'One row per trip, faithful copy of the monthly TLC Parquet files';

CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.TAXI_ZONE_LOOKUP (
    locationid              NUMBER,
    borough                 VARCHAR,
    zone                    VARCHAR,
    service_zone            VARCHAR,
    -- Absent from the file, filled by COPY INTO
    _source_file            VARCHAR,
    _loaded_at              TIMESTAMP_NTZ
)
COMMENT = 'TLC taxi zones, one row per location id';
