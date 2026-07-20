# Object Model: DDL Templates

Exact DDL the prompts should resolve to. Identical to the repo `solutions/` files.
Always `CREATE OR REPLACE`. Assume `USE DATABASE STREAMING_HOL; USE SCHEMA LOGS; USE WAREHOUSE HOL_WH;`.

## BRONZE_LOGS
```sql
CREATE OR REPLACE TABLE BRONZE_LOGS (
  PAYLOAD   VARIANT,
  LANDED_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## SILVER_LOGS
```sql
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
```

## GOLD_SERVICE_HEALTH
```sql
CREATE OR REPLACE DYNAMIC TABLE GOLD_SERVICE_HEALTH
  TARGET_LAG = '1 minute'
  WAREHOUSE  = HOL_WH
AS
SELECT
  service,
  TIME_SLICE(event_ts, 1, 'MINUTE')                            AS minute_bucket,
  COUNT(*)                                                      AS request_count,
  COUNT_IF(status_code >= 500)                                  AS error_count,
  ROUND(COUNT_IF(status_code >= 500) / NULLIF(COUNT(*), 0), 4)  AS error_rate,
  APPROX_PERCENTILE(latency_ms, 0.95)                           AS p95_latency_ms
FROM SILVER_LOGS
GROUP BY service, TIME_SLICE(event_ts, 1, 'MINUTE');
```

## SERVICE_HEALTH_SERVING (interactive table)
An Interactive Table is created with `CREATE INTERACTIVE TABLE`. It is NOT a
Dynamic Table with a flag — `CREATE DYNAMIC TABLE ... IS_INTERACTIVE=TRUE` is
invalid and will fail. Consult the bundled `snowflake-interactive` skill for
authoritative syntax.
```sql
CREATE OR REPLACE INTERACTIVE TABLE SERVICE_HEALTH_SERVING
  CLUSTER BY (service)
  TARGET_LAG = '1 minute'
  WAREHOUSE  = HOL_WH
AS
SELECT service, minute_bucket, request_count, error_count, error_rate, p95_latency_ms
FROM GOLD_SERVICE_HEALTH;
```
Fallback if interactive tables are unavailable in-region: same statement as a
`CREATE OR REPLACE DYNAMIC TABLE` (drop `CLUSTER BY`).

## SERVICE_HEALTH_SV (semantic view)
```sql
CREATE OR REPLACE SEMANTIC VIEW SERVICE_HEALTH_SV
  TABLES (
    health AS GOLD_SERVICE_HEALTH
      PRIMARY KEY (service, minute_bucket)
      WITH SYNONYMS = ('service health', 'service metrics')
      COMMENT = 'Per-service, per-minute Snowmart health metrics'
  )
  FACTS (
    health.request_count  AS request_count,
    health.error_count    AS error_count,
    health.error_rate     AS error_rate,
    health.p95_latency_ms AS p95_latency_ms
  )
  DIMENSIONS (
    health.service AS service
      WITH SYNONYMS = ('microservice', 'app', 'component')
      COMMENT = 'Name of the Snowmart microservice',
    health.minute_bucket AS minute_bucket
      WITH SYNONYMS = ('minute', 'time', 'timestamp')
      COMMENT = 'One-minute time bucket'
  )
  METRICS (
    health.total_requests  AS SUM(health.request_count)
      WITH SYNONYMS = ('traffic', 'volume') COMMENT = 'Total requests',
    health.total_errors    AS SUM(health.error_count)
      WITH SYNONYMS = ('errors', 'failures') COMMENT = 'Total 5xx errors',
    health.avg_error_rate  AS AVG(health.error_rate)
      WITH SYNONYMS = ('error rate') COMMENT = 'Average error rate',
    health.worst_p95_latency AS MAX(health.p95_latency_ms)
      WITH SYNONYMS = ('latency', 'slowness', 'p95') COMMENT = 'Worst p95 latency (ms)'
  )
  COMMENT = 'Snowmart service health for the real-time SRE co-pilot';
```
