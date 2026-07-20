-- =====================================================================
-- 02_silver.sql  (backup for the Part 2 prompt)
-- Parse the raw VARIANT into typed columns, drop heartbeats, dedupe on
-- event_id. Dynamic Table with a 1-minute target lag.
-- =====================================================================
USE DATABASE STREAMING_HOL;
USE SCHEMA   LOGS;

CREATE OR REPLACE DYNAMIC TABLE SILVER_LOGS
  TARGET_LAG = '1 minute'
  WAREHOUSE  = HOL_WH
AS
SELECT
  PAYLOAD:event_id::string        AS event_id,
  PAYLOAD:ts::timestamp_ntz       AS event_ts,
  PAYLOAD:service::string         AS service,
  PAYLOAD:level::string           AS level,
  PAYLOAD:status_code::int        AS status_code,
  PAYLOAD:latency_ms::int         AS latency_ms,
  PAYLOAD:http_method::string     AS http_method,
  PAYLOAD:endpoint::string        AS endpoint,
  PAYLOAD:region::string          AS region,
  PAYLOAD:pod::string             AS pod,
  PAYLOAD:user_id::string         AS user_id,
  PAYLOAD:trace_id::string        AS trace_id,
  PAYLOAD:exception_type::string  AS exception_type,
  PAYLOAD:dependency::string      AS dependency,
  PAYLOAD:message::string         AS message,
  LANDED_TS
FROM BRONZE_LOGS
WHERE PAYLOAD:level::string <> 'HEARTBEAT'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY PAYLOAD:event_id::string ORDER BY LANDED_TS DESC
) = 1;
