-- =====================================================================
-- 04_serving.sql  (backup for the Part 4 prompt)
-- Low-latency serving layer. A DYNAMIC INTERACTIVE TABLE auto-refreshes off
-- the Gold metrics and stays snappy for the dashboard and agent under
-- continuous updates. Requires a CLUSTER BY; TARGET_LAG minimum is 60s;
-- refresh uses a standard warehouse (HOL_WH is standard Gen2).
-- Region: this lab runs in AWS us-west-2, where Interactive Tables are GA.
-- =====================================================================
USE DATABASE STREAMING_HOL;
USE SCHEMA   LOGS;

CREATE OR REPLACE INTERACTIVE TABLE SERVICE_HEALTH_SERVING
  CLUSTER BY (service)
  TARGET_LAG = '1 minute'
  WAREHOUSE  = HOL_WH
  COMMENT = 'Low-latency serving layer for the SRE dashboard and agent'
AS
SELECT service, minute_bucket, request_count, error_count, error_rate, p95_latency_ms
FROM GOLD_SERVICE_HEALTH;

-- OPTIONAL (production): query interactive tables through an interactive
-- warehouse for best latency:
--   CREATE INTERACTIVE WAREHOUSE HOL_IWH;
--   ALTER WAREHOUSE HOL_IWH ADD TABLES SERVICE_HEALTH_SERVING;
--   ALTER WAREHOUSE HOL_IWH RESUME;

-- FALLBACK: if interactive tables are not available in your trial region,
-- replace the statement above with a plain Dynamic Table (or just query
-- GOLD_SERVICE_HEALTH directly from the dashboard/agent):
--   CREATE OR REPLACE DYNAMIC TABLE SERVICE_HEALTH_SERVING
--     TARGET_LAG = '1 minute' WAREHOUSE = HOL_WH
--   AS SELECT * FROM GOLD_SERVICE_HEALTH;
