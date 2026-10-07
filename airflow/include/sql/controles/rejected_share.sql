-- The share of the month's trips rejected by int_trips__flagged stays under
-- the DAG's threshold. NULLIF: a month with no row yields NULL, which the
-- check treats as a failure, not as a pass.
SELECT COUNT_IF(rejection_reason IS NOT NULL) / NULLIF(COUNT(*), 0) * 100
       < {{ params.max_pct_rejected }}
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;
