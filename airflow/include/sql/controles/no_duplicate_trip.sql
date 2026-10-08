-- No two valid trips of the month share a key. True by construction today:
-- int_trips__enriched deduplicates on the same columns the MD5 key hashes.
-- The check guards against one of the two being changed without the other.
SELECT COUNT(*) = COUNT(DISTINCT trip_sk)
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__ENRICHED
WHERE source_file_month = '{{ ds }}'::date;
