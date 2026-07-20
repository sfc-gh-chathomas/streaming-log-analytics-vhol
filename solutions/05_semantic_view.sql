-- =====================================================================
-- 05_semantic_view.sql  (backup for the Part 4 prompt)
-- Business-friendly grounding for the SRE co-pilot agent. Clause order is
-- required: TABLES -> FACTS -> DIMENSIONS -> METRICS. Metrics reference facts
-- by alias.
-- =====================================================================
USE DATABASE STREAMING_HOL;
USE SCHEMA   LOGS;

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
