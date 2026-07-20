-- =====================================================================
-- 06_agent.sql  (backup for the Part 5 prompt)
-- The Snowmart SRE co-pilot. Two tools:
--   1. service_health  -> Cortex Analyst over the SERVICE_HEALTH_SV semantic view
--   2. summarize_service_incident -> stored procedure that summarizes recent
--      error logs for a service with Cortex COMPLETE
-- Run this whole file as-is if the prompt-driven build drifts.
-- =====================================================================
USE DATABASE STREAMING_HOL;
USE SCHEMA   LOGS;
USE WAREHOUSE HOL_WH;

-- ---------------------------------------------------------------------
-- 1) Root-cause tool: summarize a service's recent errors with Cortex.
--    Inside a SQL procedure, reference params/vars with a colon prefix
--    (:SERVICE_NAME, :summary) or Snowflake treats them as column names.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SUMMARIZE_SERVICE_INCIDENT(SERVICE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  summary VARCHAR;
BEGIN
  SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-4-sonnet',  -- swap for your account's model (see agent_spec.md model note)
    'You are an SRE. In 3-4 sentences, summarize the likely incident and root cause '
    || 'from these recent error logs for service ' || :SERVICE_NAME || '. '
    || 'Lead with the symptom and the suspected downstream cause.\n\nLOGS:\n' || logs
  )
  INTO :summary
  FROM (
    SELECT LISTAGG(exception_type || COALESCE(' -> ' || dependency, '') || ': ' || message, '\n')
             WITHIN GROUP (ORDER BY event_ts DESC) AS logs
    FROM SILVER_LOGS
    WHERE service = :SERVICE_NAME
      AND status_code >= 500
      AND event_ts > DATEADD('minute', -10, CURRENT_TIMESTAMP())
  );
  RETURN :summary;
END;
$$;

-- ---------------------------------------------------------------------
-- 2) The agent. Dollar-quote the spec with $spec$ and run as one statement.
--    NOTE: the procedure identifier is a BARE FQN with NO argument types.
--    "...SUMMARIZE_SERVICE_INCIDENT(VARCHAR)" fails with "Unknown user-defined function".
-- ---------------------------------------------------------------------
CREATE OR REPLACE AGENT SNOWMART_SRE
  WITH PROFILE = '{"display_name": "Snowmart SRE Co-Pilot"}'
  COMMENT = 'On-call SRE co-pilot for Snowmart service health'
FROM SPECIFICATION $spec$
{
  "models": { "orchestration": "claude-4-sonnet" },
  "instructions": {
    "response": "You are an on-call SRE co-pilot for Snowmart, a consumer shopping app. Answer questions about service health using the service_health tool over the semantic view. IMPORTANT: every time you are asked which service is worst, highest, or degrading, recompute it fresh from the most recent window (default the last 5 minutes) using the service_health tool. Do NOT reuse a service named earlier in the conversation, because conditions change minute to minute and the worst service can shift. When a service is degrading, call summarize_service_incident for that specific service to explain the likely root cause from its recent error logs. Be concise and lead with the affected service, the symptom, and the suspected cause."
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "service_health",
        "description": "Query Snowmart service health metrics (request_count, error_count, error_rate, p95_latency_ms) per service per minute from the SERVICE_HEALTH_SV semantic view."
      }
    },
    {
      "tool_spec": {
        "type": "generic",
        "name": "summarize_service_incident",
        "description": "Summarize the likely incident and root cause for a degrading service from its recent error logs.",
        "input_schema": {
          "type": "object",
          "properties": {
            "service_name": {
              "type": "string",
              "description": "The service to summarize, e.g. checkout-service"
            }
          },
          "required": ["service_name"]
        }
      }
    }
  ],
  "tool_resources": {
    "service_health": {
      "semantic_view": "STREAMING_HOL.LOGS.SERVICE_HEALTH_SV",
      "execution_environment": { "type": "warehouse", "warehouse": "HOL_WH", "query_timeout": 60 }
    },
    "summarize_service_incident": {
      "type": "procedure",
      "identifier": "STREAMING_HOL.LOGS.SUMMARIZE_SERVICE_INCIDENT",
      "execution_environment": { "type": "warehouse", "warehouse": "HOL_WH", "query_timeout": 120 }
    }
  }
}
$spec$;

-- Quick smoke test of the procedure (optional):
--   CALL SUMMARIZE_SERVICE_INCIDENT('checkout-service');

-- Chat with the agent in Snowsight: AI & ML -> Agents -> SNOWMART_SRE -> agent playground.
-- Change the orchestration model there under Orchestration -> Orchestration model
-- (that dropdown lists what your account offers, e.g. a newer Sonnet).
