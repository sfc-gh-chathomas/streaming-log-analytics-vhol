# Agent Spec: Snowmart SRE Co-Pilot

The agent is the promised payoff. It answers "what is degrading right now" over the
semantic view, and explains "why" by summarizing raw error messages with Cortex.

Agent object: `STREAMING_HOL.LOGS.SNOWMART_SRE`.

## Two tools

1. **service_health** (`cortex_analyst_text_to_sql`) grounded on the semantic view
   `SERVICE_HEALTH_SV` for structured Q&A (error rate, p95 latency, counts).
2. **summarize_service_incident** (`generic` -> stored procedure) that turns a
   service's recent error logs into a plain-English incident note with Cortex.

## Step 1: create the summarization procedure

The agent calls this procedure as its root-cause tool. It wraps the Cortex
`COMPLETE` log-summarization query so the agent can pass a service name and get back
a written summary.

```sql
CREATE OR REPLACE PROCEDURE STREAMING_HOL.LOGS.SUMMARIZE_SERVICE_INCIDENT(SERVICE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  summary VARCHAR;
BEGIN
  SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-4-sonnet',  -- swap for your account's model; see the model note below
    'You are an SRE. In 3-4 sentences, summarize the likely incident and root cause '
    || 'from these recent error logs for service ' || :SERVICE_NAME || '. '
    || 'Lead with the symptom and the suspected downstream cause.\n\nLOGS:\n' || logs
  )
  INTO :summary
  FROM (
    SELECT LISTAGG(exception_type || COALESCE(' -> ' || dependency, '') || ': ' || message, '\n')
             WITHIN GROUP (ORDER BY event_ts DESC) AS logs
    FROM STREAMING_HOL.LOGS.SILVER_LOGS
    WHERE service = :SERVICE_NAME
      AND status_code >= 500
      AND event_ts > DATEADD('minute', -10, CURRENT_TIMESTAMP())
  );
  RETURN :summary;
END;
$$;
```

Note: inside a SQL procedure body, parameters and DECLARE variables must be
referenced with a colon prefix (`:SERVICE_NAME`, `:summary`), or Snowflake treats
them as column names.

## Step 2: create the agent (exact spec)

Create the agent with SQL. Run this statement as-is; do NOT reach for a helper
script or `uv`. The spec is dollar-quoted with `$spec$`.

```sql
CREATE OR REPLACE AGENT STREAMING_HOL.LOGS.SNOWMART_SRE
  WITH PROFILE = '{"display_name": "Snowmart SRE Co-Pilot"}'
  COMMENT = 'On-call SRE co-pilot for Snowmart service health'
FROM SPECIFICATION $spec$
{
  "models": { "orchestration": "auto" },
  "instructions": {
    "response": "You are an on-call SRE co-pilot for Snowmart, a consumer shopping app. Be concise and lead with the affected service, the symptom, and the suspected cause. Use a calm, actionable incident-response tone.",
    "orchestration": "Use the service_health tool (the SERVICE_HEALTH_SV semantic view) for any question about which services are worst or degrading, error rates, p95 latency, request or error counts, and trends. Every time you are asked which service is worst, highest, or degrading, recompute it fresh from the most recent window (default the last 5 minutes); do NOT reuse a service named earlier in the conversation, because conditions change minute to minute. Find the worst service first with service_health, then call summarize_service_incident for that specific service to explain the likely root cause from its recent error logs."
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
```

### Two things that trip up agent creation (get them right the first time)

1. **Procedure identifier: bare FQN, no argument types.** In `tool_resources`, the
   procedure `identifier` must be `"STREAMING_HOL.LOGS.SUMMARIZE_SERVICE_INCIDENT"`.
   Do NOT append the signature (for example
   `"...SUMMARIZE_SERVICE_INCIDENT(VARCHAR)"`); the runtime quotes the whole string
   as one identifier and fails with "Unknown user-defined function".
2. **Dollar-quote the spec with `$spec$`, run it as one SQL statement.** Keep the
   `CREATE OR REPLACE AGENT ... FROM SPECIFICATION $spec$ ... $spec$` form. If a SQL
   tool rejects the dollar-quoted body, run the statement directly in a Snowsight
   worksheet rather than splitting or re-quoting it.

## Tone / instructions

"You are an on-call SRE co-pilot for Snowmart. Answer questions about service health
using the semantic view. When a service is degrading, summarize the likely incident
from its recent error logs. Be concise and lead with the affected service, the
symptom, and the suspected cause."

## Where to chat with the agent

CoCo creates the agent object in Snowflake, but the attendee talks to it in Snowsight,
not in CoCo Desktop. Tell them: **Snowsight -> AI & ML -> Agents -> SNOWMART_SRE ->
agent playground**. That playground is where they ask the demo questions below. (The
Snowflake Intelligence chat app also works but needs extra `SNOWFLAKE_INTELLIGENCE`
database setup, so the Agents playground is the simplest path for the lab.)

## Model note

- The agent's orchestration model is separate from the CoCo Desktop model picker. The
  model at the top of CoCo is what builds the lab; it is not the agent's brain. The
  agent's model is set in `models.orchestration` in the spec above.
- **Agent orchestration uses a restricted, account-specific allowed-models list that is
  narrower than Cortex COMPLETE.** Pinning a model that is not on it (for example
  `claude-4-sonnet`) fails with "X is not an allowed model for Agent". So set
  `models.orchestration` to `"auto"` and let Snowflake choose an allowed model. To pin a
  specific one such as Sonnet 5, open the agent in Snowsight -> **Orchestration ->
  Orchestration model** and pick from the dropdown; that dropdown is the authoritative
  per-account list of allowed agent models.
- **The summarization procedure's `COMPLETE` call is a different check** with its own
  allowed-models list. It needs a concrete model literal (COMPLETE does not accept `auto`).
  Use a model the account offers. Quick check in a worksheet:
  `SELECT SNOWFLAKE.CORTEX.COMPLETE('<model>', 'hi');` If it returns text, that model works;
  swap the literal in the `SUMMARIZE_SERVICE_INCIDENT` procedure to match. If no Claude model
  is available in-region, enable cross-region inference once
  (`ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';`) or use `openai-gpt-5`.

## Pick the worst service automatically

For "what is degrading right now", the agent can find the top offender first:

```sql
SELECT service, error_rate, p95_latency_ms
FROM STREAMING_HOL.LOGS.GOLD_SERVICE_HEALTH
WHERE minute_bucket >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
QUALIFY ROW_NUMBER() OVER (ORDER BY error_rate DESC, p95_latency_ms DESC) = 1;
```

## Baseline dialogue (before the fault, Part 5)

Right after the agent is built, traffic is healthy and there is no incident yet, so ask
orientation questions that just prove the agent reads the live data and speaks in service
terms. Do NOT ask for a root cause or an incident report here: there is nothing wrong yet.

1. "Which services are you monitoring for Snowmart?" -> lists the services from the semantic view.
2. "In the last 5 minutes, what is the error rate and p95 latency for each service?" -> a per-service table.
3. "Which service has the highest error rate right now, and is that within a normal range?" -> usually recommendation-service at a low baseline rate.
4. "Show me the request volume by service over the last few minutes." -> per-service counts.

## Incident dialogue (after the fault, Part 7)

Ask these AFTER the incident is injected (Part 7). This is the troubleshoot-then-report arc:
find the worst service, get root cause, check blast radius, then draft the RCA. Keep the
prompts time-bounded to the last few minutes so the agent evaluates the current incident, not
the earlier baseline.

1. "In the last 5 minutes, which service is worst right now?" -> checkout-service / payment-service.
2. "Give me the root cause for the single worst service." -> summary citing downstream payment-service 503s / timeouts.
3. "Is anything upstream or related affected?" -> cart-service and checkout-service correlated on payment.
4. "What should I check first to mitigate?" -> payment-service latency and any recent deploy.
5. "Draft an RCA report I can post to the on-call channel: summary, impact, timeline, suspected root cause, and next steps." -> a short shareable incident writeup.

Anchoring tip: if you asked the agent earlier (during the healthy baseline) and it named a
different service (for example recommendation-service, which is the noisiest at baseline),
it may keep referring to that service. Re-ask with an explicit recent window ("in the last
5 minutes, which service is worst right now?"), or start a new conversation, so it recomputes
against the current incident. The agent's response instruction tells it to recompute every
time, but a tight time window makes it unambiguous.

The Log Data Producer fault (`checkout_cascade`) is built so these answers are
truthful: payment-service times out (504 / circuit breaker), checkout-service logs
"downstream payment-service call failed", cart-service takes a mild hit, others stay
healthy. For a clean demo, run the producer healthy for a few minutes first so the
baseline is quiet, THEN inject the fault, so checkout/payment clearly become the worst.
