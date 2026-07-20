-- =====================================================================
-- 03_gold.sql  (backup for the Part 3 prompt)
-- The AI-ready dataset: per service, per 1-minute bucket health metrics.
-- Note: APPROX_PERCENTILE is non-deterministic, so this Dynamic Table runs
-- in FULL refresh mode (not incremental). That is fine at lab scale with a
-- 1-minute lag; for large-scale incremental refresh, drop p95 or precompute it.
-- =====================================================================
USE DATABASE STREAMING_HOL;
USE SCHEMA   LOGS;

CREATE OR REPLACE DYNAMIC TABLE GOLD_SERVICE_HEALTH
  TARGET_LAG = '1 minute'
  WAREHOUSE  = HOL_WH
AS
SELECT
  service,
  TIME_SLICE(event_ts, 1, 'MINUTE')                                AS minute_bucket,
  COUNT(*)                                                          AS request_count,
  COUNT_IF(status_code >= 500)                                      AS error_count,
  ROUND(COUNT_IF(status_code >= 500) / NULLIF(COUNT(*), 0), 4)      AS error_rate,
  APPROX_PERCENTILE(latency_ms, 0.95)                               AS p95_latency_ms
FROM SILVER_LOGS
GROUP BY service, TIME_SLICE(event_ts, 1, 'MINUTE');
