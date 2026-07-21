---
name: coco-streaming-vhol
description: "Build the Snowmart real-time log analytics pipeline for the Snowpipe Streaming + Dynamic Tables VHOL. Loads the exact object model, naming, and DDL patterns so short prompts produce consistent objects. Use when: building the streaming pipeline, Bronze/Silver/Gold, semantic view, SRE agent, or dashboard for this lab. Triggers: streaming vhol, snowmart, bronze logs, silver logs, gold service health, sre co-pilot, log pipeline."
---

# Snowmart Streaming Log Analytics VHOL

You are helping a workshop attendee build a real-time log analytics pipeline on
Snowflake using CoCo (Cortex Code). The scenario: **Snowmart**, a consumer shopping
app, streams service logs into Snowflake. We refine them with Dynamic Tables,
serve them low-latency, and put an AI SRE co-pilot on top.

**Golden rule:** when the attendee asks to build a layer, produce the object with
the EXACT names, columns, and settings in the Object Model below. Do not rename
columns, change grain, or alter target lag. Consistency is what keeps every
attendee's pipeline working through the whole lab. Always use `CREATE OR REPLACE`
so a re-run is safe. After creating an object, run its Checkpoint query and report
the result.

## Fixed context

- Region: AWS `us-west-2` (US West, Oregon). Interactive Tables are supported here,
  so the serving layer is a dynamic interactive table (no fallback needed).
- Database `STREAMING_HOL`, schema `LOGS`, warehouse `HOL_WH` (Gen2, standard), and
  `BRONZE_LOGS` are created by YOU from the Part 1 prompt (see the object model and
  workflow). `00_bootstrap.sql` only creates the `VHOLuser` login, its network policy,
  and its PAT, and sets the account to UTC. Once the environment exists, always
  `USE DATABASE STREAMING_HOL; USE SCHEMA LOGS; USE WAREHOUSE HOL_WH;` first.
- The account is set to UTC (`ALTER ACCOUNT SET TIMEZONE='UTC'` in bootstrap). The
  producer emits UTC event times, so all freshness/lag math uses `CURRENT_TIMESTAMP()`
  and stays consistent. Do not mix in local-timezone timestamps.
- The Log Data Producer streams JSON log objects into `BRONZE_LOGS.PAYLOAD`
  (VARIANT). Do not modify the producer.
- Setup check: if the attendee asks to test the connection and confirm this skill is
  loaded, run `SELECT CURRENT_ACCOUNT() AS account, CURRENT_USER() AS user, CURRENT_ROLE() AS role;`,
  report the values (expect user `VHOLUSER`, role `ACCOUNTADMIN`), and confirm the
  `coco-streaming-vhol` skill is active (you are running it).

## Object Model (single source of truth)

### Environment (created from the Part 1 prompt)
- Database `STREAMING_HOL`, schema `LOGS`.
- Warehouse `HOL_WH`: standard **Gen2** (`GENERATION='2'`), `WAREHOUSE_SIZE='XSMALL'`,
  `AUTO_SUSPEND=60`, `AUTO_RESUME=TRUE`, `INITIALLY_SUSPENDED=TRUE`.

### BRONZE_LOGS (raw streaming target)
Columns: `PAYLOAD VARIANT`, `LANDED_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()`.
The Snowpipe Streaming SDK auto-creates the default pipe `BRONZE_LOGS-STREAMING`.
Each PAYLOAD object has: event_id, ts, service, level, status_code, latency_ms,
http_method, endpoint, region, pod, user_id, trace_id, exception_type (errors),
dependency (some errors), message.
Freshness note: Snowpipe Streaming does NOT reliably apply the `LANDED_TS` column
DEFAULT to streamed rows, so `LANDED_TS` can be stale/empty. For any Bronze
freshness or "seconds ago" measurement, use the payload event time instead:
`DATEDIFF('second', TO_TIMESTAMP_TZ(PAYLOAD:ts::string), CURRENT_TIMESTAMP())`
(the account is UTC, so this lines up with CURRENT_TIMESTAMP()).

### SILVER_LOGS (Dynamic Table, TARGET_LAG='1 minute', WAREHOUSE=HOL_WH)
Flatten PAYLOAD to typed columns: event_id, event_ts (TIMESTAMP_NTZ from ts),
service, level, status_code, latency_ms, http_method, endpoint, region, pod,
user_id, trace_id, exception_type, dependency, message, LANDED_TS.
Rules: filter out `level = 'HEARTBEAT'`; deduplicate keeping one row per
`event_id` (ROW_NUMBER over LANDED_TS DESC).

### GOLD_SERVICE_HEALTH (Dynamic Table, TARGET_LAG='1 minute', WAREHOUSE=HOL_WH)
Grain: one row per `service` per 1-minute bucket. Columns: service,
minute_bucket (`TIME_SLICE(event_ts,1,'MINUTE')`), request_count (COUNT(*)),
error_count (COUNT_IF(status_code>=500)), error_rate (error_count/request_count
rounded 4dp), p95_latency_ms (APPROX_PERCENTILE(latency_ms,0.95)).
Note: APPROX_PERCENTILE makes this a FULL-refresh DT; that is expected.

### SERVICE_HEALTH_SERVING (Interactive Table)
An **Interactive Table** is its own object type, created with
`CREATE [OR REPLACE] INTERACTIVE TABLE`. It is NOT a Dynamic Table with a flag.
Use the bundled `snowflake-interactive` skill for authoritative Interactive Table
syntax before generating this DDL. The correct form is:

```sql
CREATE OR REPLACE INTERACTIVE TABLE SERVICE_HEALTH_SERVING
  CLUSTER BY (service)
  TARGET_LAG = '1 minute'
  WAREHOUSE  = HOL_WH
AS
SELECT service, minute_bucket, request_count, error_count, error_rate, p95_latency_ms
FROM GOLD_SERVICE_HEALTH;
```

Do NOT write `CREATE DYNAMIC TABLE ... IS_INTERACTIVE=TRUE` — there is no
`IS_INTERACTIVE` clause and that statement will fail. This is the low-latency read
layer for the dashboard and agent. If Interactive Tables are unavailable in the
region, fall back to a plain Dynamic Table with the same name and select list.

### SERVICE_HEALTH_SV (Semantic View over GOLD_SERVICE_HEALTH)
Clause order TABLES -> FACTS -> DIMENSIONS -> METRICS.
- Logical table `health` = GOLD_SERVICE_HEALTH, PRIMARY KEY (service, minute_bucket).
- Facts: request_count, error_count, error_rate, p95_latency_ms.
- Dimensions: service (synonyms microservice/app/component), minute_bucket.
- Metrics: total_requests=SUM(request_count), total_errors=SUM(error_count),
  avg_error_rate=AVG(error_rate), worst_p95_latency=MAX(p95_latency_ms).

## Workflow

Follow the attendee's lead through these steps. Each maps to one prompt.

0. **Producer setup** — two tasks the attendee will prompt for:
   - **Detect the OS first.** Check the platform before running any shell command and
     use the matching interpreter paths. This lab runs on macOS, Linux, and Windows,
     and only the Python invocation differs:
     - macOS / Linux: create with `python3 -m venv .venv`; the venv interpreter is
       `.venv/bin/python` and pip is `.venv/bin/pip`.
     - Windows: create with `python -m venv .venv` (use `python`, not `python3`); the
       venv interpreter is `.venv\Scripts\python.exe` and pip is
       `.venv\Scripts\pip.exe`. Do NOT use `uv` or the `.venv/bin/...` (POSIX) paths on
       Windows.
   - **Install deps:** create the virtual environment at the repo root and install into
     it (`<venv-pip> install -r producer/requirements.txt`). macOS Homebrew Python is
     externally managed (PEP 668), so a venv is required there. Run the producer with
     the venv interpreter (`<venv-python> producer/log_data_producer.py ...`).
   - **Build `producer/profile.json`:** `user` is always `VHOLuser` (the bootstrap user; the
     PAT belongs to it), so set it literally, do not query it. Get `account` by running SQL on
     the active connection (the SQL tool uses whatever connection is active in CoCo Desktop):
     `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account;`
     Do NOT use `snowflake_connections_list` or shell out to the `cortex` CLI for the account:
     those resolve to the CLI's default connection (often a different account, e.g. an internal
     one), NOT the VHOLuser trial connection active in CoCo Desktop. Derive `url` as
     `https://<account>.snowflakecomputing.com:443`, and write `producer/profile.json`
     matching `producer/profile.example.json` with `authorization_type: "PAT"`. Fill
     `personal_access_token` by reading `secret.pat` (repo root) **inside a shell command**
     (e.g., a `python3 -c` one-liner that opens the file and writes the JSON) so the token
     is NEVER printed to chat and NEVER read into your context. Do not echo the token. If
     `secret.pat` is missing, ask the attendee to paste their `vhol_pat` token
     into it first.
1. **Environment + producer** — create the database `STREAMING_HOL`, schema `LOGS`, the
   Gen2 XSMALL warehouse `HOL_WH`, and `BRONZE_LOGS`; set context; confirm the SDK
   profile; then **start the producer in the background** (Bash `run_in_background`) with
   the venv interpreter, e.g. `<venv-python> producer/log_data_producer.py --profile producer/profile.json --rps 200`
   so it keeps streaming while later layers build; verify freshness (Checkpoint B). For the
   incident (Part 7), stop the running producer and restart it in the background with
   `--fault checkout_cascade --fault-after 30`.
2. **Silver** — create SILVER_LOGS per the model. Checkpoint S.
3. **Gold** — create GOLD_SERVICE_HEALTH per the model. Checkpoint G.
4. **Serving + semantic view** — create SERVICE_HEALTH_SERVING and SERVICE_HEALTH_SV.
   For the serving layer, load the bundled `snowflake-interactive` skill and use
   `CREATE INTERACTIVE TABLE` (never `DYNAMIC TABLE ... IS_INTERACTIVE`).
5. **Agent** — build the SRE co-pilot exactly as in `references/agent_spec.md`: first
   create the `SUMMARIZE_SERVICE_INCIDENT` procedure, then run the `CREATE OR REPLACE
   AGENT ... FROM SPECIFICATION $$ ... $$` statement as-is. Do NOT use `uv` or a
   helper script. Dollar-quote the spec with `$$`, NOT a named tag like `$spec$`: CoCo's
   SQL execution path rejects named dollar-quote tags (errors with `unexpected '$spec'`),
   while `$$` runs cleanly and the spec JSON never contains `$$`. The procedure
   `identifier` in `tool_resources` must be a bare FQN with NO argument types
   (`STREAMING_HOL.LOGS.SUMMARIZE_SERVICE_INCIDENT`, not `...(VARCHAR)`). Set
   `models.orchestration` to `"auto"`: agent orchestration has a restricted,
   account-specific allowed-models list, so a pinned model like `claude-4-sonnet` can
   fail with "not an allowed model for Agent". Include both `instructions.response`
   (tone/style) and `instructions.orchestration` (tool routing + the
   recompute-worst-service-from-recent-window rule) so the agent has core behavior and
   response style configured. **To fix or change the agent, re-run the full
   `CREATE OR REPLACE AGENT ... FROM SPECIFICATION` statement (a clean recreate). Do NOT use
   a workspace-file edit/redeploy path** (it fails with "Could not resolve workspace file ...
   cortex-project.yaml" because this agent is created from SQL, not tracked in a workspace).
   After creating it, tell the attendee to chat with the
   agent in **Snowsight -> AI & ML -> Agents -> SNOWMART_SRE -> agent playground** (not in
   CoCo Desktop). To pin a specific model such as Sonnet 5, open the agent in Snowsight,
   click **Edit**, select the **Orchestration** section, and choose from the **Orchestration
   model** dropdown (it only appears after Edit); or re-run CREATE OR REPLACE AGENT with a
   different `models.orchestration`.
6. **Dashboard** — build the Streamlit app (see references/dashboard_spec.md). This is a
   Streamlit in Snowflake app: NO `snow` CLI and no local server. Write
   `dashboard/streamlit_app.py`, then either create it via `CREATE STREAMLIT` in
   `STREAMING_HOL.LOGS` on `HOL_WH`, or tell the attendee to make it in Snowsight ->
   Projects -> Streamlit -> + Streamlit App and paste the file. It uses
   `get_active_session()` and runs on the warehouse.

## Checkpoints

- **B (bronze freshness):**
  `SELECT PAYLOAD, DATEDIFF('second', TO_TIMESTAMP_TZ(PAYLOAD:ts::string), CURRENT_TIMESTAMP()) AS seconds_ago FROM BRONZE_LOGS ORDER BY TO_TIMESTAMP_TZ(PAYLOAD:ts::string) DESC LIMIT 10;`
  Show the raw JSON `PAYLOAD` column (not parsed fields) so the room sees raw logs landing,
  plus `seconds_ago`. Expect rows landing within seconds. (Uses the payload event time, which
  measures true produce-to-queryable latency.)
- **S (silver):** `SELECT COUNT(*), COUNT(DISTINCT event_id) FROM SILVER_LOGS;`
  Counts equal (deduped); no HEARTBEAT rows.
- **G (gold):** `SELECT * FROM GOLD_SERVICE_HEALTH ORDER BY minute_bucket DESC LIMIT 20;`
  One row per service per minute with error_rate and p95_latency_ms.

## References

- `references/object_model.md` — full DDL templates (also in the repo `solutions/`).
- `references/agent_spec.md` — agent grounding + Cortex log-summarization prompt.
- `references/dashboard_spec.md` — Streamlit dashboard pattern.

## Stopping Points

- Confirm BRONZE_LOGS exists before starting the producer.
- After each layer, run its Checkpoint before moving on.

## Output

The Snowmart pipeline: BRONZE_LOGS, SILVER_LOGS, GOLD_SERVICE_HEALTH,
SERVICE_HEALTH_SERVING, SERVICE_HEALTH_SV, an SRE agent, and a real-time dashboard.
