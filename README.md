# Power AI Agents with Continuous Streaming Data Pipelines

Virtual Hands-On Lab. You build a real-time log analytics pipeline for **Snowmart**, a
consumer shopping app: stream service logs into Snowflake with Snowpipe Streaming,
refine them with Dynamic Tables, serve them low-latency, and put an AI SRE co-pilot
and a live dashboard on top. You build it by prompting CoCo (Cortex Code), not by
pasting SQL.

## What you need

- A Snowflake trial account (you will be ACCOUNTADMIN). **Create it on AWS in the
  US West (Oregon) `us-west-2` region.** This lab uses Interactive Tables, which are
  available in `us-west-2`; other regions may not support them.
- CoCo Desktop (Cortex Code) installed
  ([download](https://www.snowflake.com/en/product/snowflake-coco/downloads/)).
- Git and Python 3.9+ locally for the lab files and the Log Data Producer.

## Repo layout

```
README.md       this file — setup + the full lab walkthrough
producer/       Log Data Producer (Snowpipe Streaming) + requirements
solutions/      SQL answer key (bootstrap + backups + cleanup)
dashboard/      Streamlit in Snowflake dashboard
.snowflake/cortex/skills/coco-streaming-vhol/   the VHOL skill (auto-loads in CoCo)
```

## Setup

Do these **in order**. You clone the repo first (CoCo Desktop asks for a project folder
when it launches), then bootstrap the account to mint the `VHOLuser` login and its
Programmatic Access Token (PAT). You connect CoCo Desktop *as* `VHOLuser` with that PAT,
and the producer uses the same PAT. One identity for everything.

### A. Get the lab files

1. **Clone this repository** to your machine — you'll open it as your project in CoCo
   Desktop and run the producer from it:
   ```
   git clone https://github.com/sfc-gh-chathomas/streaming-log-analytics-vhol.git
   cd streaming-log-analytics-vhol
   ```
   (No git? Use the green **Code -> Download ZIP** button on the repo page and unzip it.)

### B. Bootstrap the account (Snowsight)

2. Log in to your trial in **Snowsight as your signup user** (you are ACCOUNTADMIN) and
   run the SQL below once in a worksheet (identical to `solutions/00_bootstrap.sql`). It
   sets the account to UTC and creates the lab user `VHOLuser`, its network policy, and
   its PAT `vhol_pat`. The database, schema, warehouse, and tables are built later by
   prompting CoCo (Part 1). **Copy the `token_secret` from the last statement's results
   grid — it is shown only once.** Save it as the **only contents** of a file named
   `secret.pat` in the **repo root** you cloned in step 1 (gitignored). You'll paste this
   same token into the CoCo connection (step 4), and CoCo reads `secret.pat` when it builds
   the producer profile.
   ```sql
   USE ROLE ACCOUNTADMIN;

   -- Standardize on UTC so the producer's event times (UTC) and CURRENT_TIMESTAMP()
   -- agree. Without this, per-layer freshness math is off by your local UTC offset.
   ALTER ACCOUNT SET TIMEZONE = 'UTC';

   -- Lab user for the CoCo connection and the producer. ACCOUNTADMIN so CoCo can
   -- build the rest of the pipeline (database, warehouse, tables) via prompts.
   CREATE USER IF NOT EXISTS VHOLuser
     DEFAULT_ROLE = ACCOUNTADMIN
     COMMENT = 'Streaming VHOL lab user';
   GRANT ROLE ACCOUNTADMIN TO USER VHOLuser;

   -- PATs require the user to be under a network policy, so open a permissive one.
   CREATE NETWORK POLICY IF NOT EXISTS vhol_np ALLOWED_IP_LIST = ('0.0.0.0/0');
   ALTER USER VHOLuser SET NETWORK_POLICY = vhol_np;

   ALTER USER VHOLuser
     ADD PROGRAMMATIC ACCESS TOKEN vhol_pat
       ROLE_RESTRICTION = 'ACCOUNTADMIN'
       DAYS_TO_EXPIRY = 7
       COMMENT = 'Streaming VHOL lab token';
   -- >>> Copy the token_secret value now (shown once). <<<
   -- To revoke later: ALTER USER VHOLuser REMOVE PROGRAMMATIC ACCESS TOKEN vhol_pat;
   ```

### C. Connect CoCo Desktop (as VHOLuser)

3. **Install and launch CoCo Desktop.** When it asks for a project folder, open the repo
   folder you cloned in step 1.
   - Download: https://www.snowflake.com/en/product/snowflake-coco/downloads/
   - Setup/docs: https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-desktop

   CoCo Desktop is available to all accounts; usage is billed on token consumption, so it
   runs on your trial credits.
4. **Add the connection.** Click the **Connections** button in CoCo Desktop to open the
   connection manager and add a new connection:
   - **Account identifier:** your trial's `ORG-ACCOUNT`
   - **User:** `VHOLuser`
   - **Auth:** paste the `vhol_pat` token from step 2. If the wizard lists **Programmatic
     Access Token**, use that. If it doesn't (some CoCo Desktop versions only show
     Password / Key Pair / External Browser), choose **Password** and paste the PAT into
     the **password** field — a PAT is a drop-in replacement for a password in Snowflake
     drivers, and it skips MFA.

   Select it so it becomes the **active** connection. That's all you need — CoCo Desktop
   uses the active connection for both running SQL and the agent's Cortex inference.

   Alternatively, if you'd rather not use the wizard, add the connection directly to
   `~/.snowflake/connections.toml` and CoCo will list it:
   ```toml
   [connections.vhol]
   account = "ORG-ACCOUNT"
   user = "VHOLuser"
   authenticator = "PROGRAMMATIC_ACCESS_TOKEN"
   token = "<paste vhol_pat>"
   ```
5. **Pick the model.** In the model selector, use the **latest Claude Sonnet (Sonnet 5
   preferred)** or the **latest Claude Opus**; fall back to the **latest GPT** (e.g.,
   `openai-gpt-5`) if no Claude is available. The picker only lists models your account
   and region can use, so if it's listed you have access — don't pick one that isn't.
6. **Validate the connection and skill.** In CoCo's chat, run:
   > Test my Snowflake connection and make sure the coco-streaming-vhol skill is loaded as part of this lab.

   CoCo should run a quick query (confirming your `VHOLuser` trial connection works) and
   confirm the `coco-streaming-vhol` skill is active. If the skill isn't loaded, add it:
   Agent Settings → Skills → Local Skills → **+ Add Local Skill**, and pick the repo folder.

### D. Build the producer (prompt-driven)

CoCo sets up the producer for you, using the `secret.pat` you saved during bootstrap
(step 2), so the token never appears in chat.

7. **Set up the producer environment.** Prompt CoCo:
   > Detect my operating system, then create a Python virtual environment named .venv in the repo root and install the Log Data Producer dependencies from producer/requirements.txt into it. Use the correct interpreter path for my OS (.venv/bin on macOS/Linux, .venv\Scripts on Windows). On macOS, Homebrew Python is externally managed, so a venv is required.
8. **Create your streaming profile.** Prompt CoCo:
   > Create producer/profile.json for the Snowpipe Streaming SDK: use my active connection's
   > account and user VHOLuser with authorization_type PAT, and fill personal_access_token by
   > reading secret.pat (repo root) with a shell command so the token is never printed in chat.

## Run the lab

**Scenario.** You're Sam Rivera, on call at **Snowmart**, a consumer shopping app. Every
service already emits logs, but today they land in a separate tool that's expensive, short
on retention, and cut off from the rest of your Snowflake data. Your pager is about to go
off. Over the next hour you'll rebuild this the Snowflake way — point the producer at
Snowflake, refine the stream, and put an AI co-pilot and a live dashboard on top — by
prompting CoCo, not by writing SQL. Each part is one prompt plus a checkpoint; you describe
each step in plain English, and the `coco-streaming-vhol` skill keeps everyone's object
names identical so each layer lines up with the next. If an output ever drifts, the matching
file in `solutions/` is the answer key.

### Part 1 — Connect the producer

You already have a Log Data Producer (in the real world: a log shipper, an OTel collector,
or an app producer). The only new step is pointing it at Snowflake — so first build the
landing zone:
> Set up the lab environment: create database STREAMING_HOL, schema LOGS, a Gen2 XSMALL
> warehouse HOL_WH, and the raw streaming landing table BRONZE_LOGS (a VARIANT payload
> column and a landed timestamp default current_timestamp). Set STREAMING_HOL, LOGS, and
> HOL_WH as my active context.

Now start the producer. It emits one JSON log per line across the Snowmart services and
streams straight into BRONZE_LOGS via the Snowpipe Streaming SDK — CoCo runs it in the
background so data keeps landing while you build the next layers:
> Start the Log Data Producer in the background using the project's venv Python (the
> .venv interpreter you created for my OS): run producer/log_data_producer.py with
> --profile producer/profile.json --rps 200.

Then see how fresh it is:
> Show me the newest rows landing in BRONZE_LOGS and how many seconds ago each arrived.

**Checkpoint:** rows land within a few seconds — producer to queryable in about 5s. That's
Snowpipe Streaming, on flat ingest-based pricing.

### Part 2 — Silver: clean the stream

Raw JSON isn't what you want to query mid-incident, so Silver does three jobs at once — all
declaratively, no scheduled jobs, just a Dynamic Table that keeps itself fresh:
- **Flatten + type** the VARIANT payload into real columns you can filter and aggregate.
- **Filter** out `HEARTBEAT` keepalive events — noise that would skew the counts.
- **Dedupe** on `event_id`, keeping the latest by `LANDED_TS` — a streaming pipeline can
  deliver the same event more than once (retries, replays, restarts), so Silver enforces
  one row per event.

> Create a Dynamic Table SILVER_LOGS from BRONZE_LOGS that flattens the JSON payload into
> typed columns — event_id, event_ts (from the ts field), service, level, status_code,
> latency_ms, http_method, endpoint, region, pod, user_id, trace_id, exception_type,
> dependency, message — plus the LANDED_TS. Filter out level = 'HEARTBEAT' and dedupe on
> event_id, keeping the latest row by LANDED_TS. Target lag 1 minute, warehouse HOL_WH.

**Checkpoint:** row count equals the distinct `event_id` count (deduped) and no HEARTBEAT
rows remain — clean, typed, and still fresh within a minute.

### Part 3 — Gold: service-health rollups

During an incident you don't read individual logs — you read service health. Roll up to the
metrics that matter:
> Create a Dynamic Table GOLD_SERVICE_HEALTH from SILVER_LOGS, grouped by service and a
> one-minute time bucket (minute_bucket), with request_count, error_count (status_code >=
> 500), error_rate (error_count / request_count, guarded against divide-by-zero), and
> p95_latency_ms. Target lag 1 minute, warehouse HOL_WH.

**Checkpoint:** one row per service per minute, with `error_rate` and `p95_latency_ms`. This
is the AI-ready dataset: health per service per minute.

### Part 4 — Serve it + semantic view

Two consumers are coming — a dashboard and an agent — and both need fast reads under constant
updates. That's what an Interactive Table is for:
> Create an Interactive Table SERVICE_HEALTH_SERVING over GOLD_SERVICE_HEALTH for
> low-latency reads by the dashboard and agent, clustered by service, target lag 1 minute,
> warehouse HOL_WH.

The agent also needs to understand the data in business terms, so give it a semantic view:
> Create a semantic view SERVICE_HEALTH_SV over GOLD_SERVICE_HEALTH with facts request_count,
> error_count, error_rate, and p95_latency_ms; dimensions service and minute_bucket; and
> metrics for total requests, total errors, average error rate, and worst p95 latency. Add
> natural-language synonyms (e.g. latency, errors, traffic) so an agent can query it in
> plain English.

**Checkpoint:** query the semantic view for total requests, total errors, and average
error rate by service. Now the data speaks the language you use on call.

### Part 5 — Build the SRE co-pilot agent

This is the moment — your pager just fired. Instead of grepping dashboards, you ask an AI
co-pilot. It answers "what's degrading right now" over the semantic view and explains "why"
by summarizing raw errors with Cortex:
> Build an on-call SRE co-pilot agent grounded on the SERVICE_HEALTH_SV semantic view.
> When a service is degrading, it should also summarize the recent error messages for that
> service into a plain-English incident note using Cortex.

CoCo builds the agent **object** in Snowflake (it creates a helper procedure and the
`SNOWMART_SRE` agent). To actually **talk** to it, go to **Snowsight → AI & ML → Agents**,
click **SNOWMART_SRE**, and use the **agent playground** on its detail page. Ask it, in turn:
1. In the last 5 minutes, which services have the highest error rates right now?
2. Give me the root cause for the single worst service.
3. Is anything upstream or related affected?
4. What should I check first?
5. Draft an incident note I can post to the on-call channel.

These land hardest once the incident is live (Part 7). Keep the window explicit ("in the
last 5 minutes") so the agent evaluates current conditions. If it keeps naming a service
from an earlier answer (the baseline worst is usually recommendation-service), re-ask with
the recent window or start a new conversation so it recomputes against the live data.

**Model note.** The agent has its own brain, set separately from CoCo's model picker (the
model at the top of CoCo builds the lab; it is not the agent). We default the agent to
`claude-4-sonnet`. To use a newer model like Sonnet 5, open the agent in Snowsight →
**Orchestration → Orchestration model** and pick whatever your account lists there; that
dropdown is the authoritative per-account list.

You just went from paged to a written incident hypothesis in a few questions, on data about
a minute old, without writing a single query.

### Part 6 — Real-time dashboard

The agent is for asking; the dashboard is for watching — and it's where the latency story
becomes honest and visible:
> Build a Streamlit dashboard for the streaming VHOL with three panels: (1) a per-layer
> freshness meter for BRONZE_LOGS, SILVER_LOGS, GOLD_SERVICE_HEALTH, and
> SERVICE_HEALTH_SERVING; (2) a live raw log feed from BRONZE_LOGS with how many seconds
> ago each row landed; (3) error rate and p95 latency per service from
> SERVICE_HEALTH_SERVING with the worst service highlighted. Auto-refresh every 5 seconds.

**Checkpoint:** the freshness meter shows Bronze at a few seconds (Snowpipe Streaming) while
Silver/Gold/Serving sit near a minute (the Dynamic Table target lag you chose). That contrast
is the point — seconds-fresh raw for immediacy, minute-fresh aggregates for analytics and the
agent — and the aggregates read from an Interactive Table, so it stays snappy while data lands.

### Part 7 — Trigger the incident

Now make it real. Restart the producer with a fault on checkout-service — a latency spike and
an error burst, exactly what paged you:
> Stop the running producer and restart it in the background with the incident fault:
> run producer/log_data_producer.py (with the project's venv Python) with --profile
> producer/profile.json --rps 200 --fault checkout_cascade --fault-after 30.

Watch all three surfaces at once: raw errors hit BRONZE_LOGS in seconds, Silver/Gold update
within a minute, and the dashboard line for checkout-service spikes. Give Gold a minute to
aggregate the spike, then ask the agent (in the agent playground):
> In the last 5 minutes, which service is worst right now, and what is the root cause?

The visual spike and the AI explanation land together — that's the payoff.

## How the skill knows all this

You typed plain-English prompts, yet every table landed with the same names, columns, and
refresh settings. That consistency comes from the **`coco-streaming-vhol` skill** in this
repo (`.snowflake/cortex/skills/coco-streaming-vhol/`). CoCo loads it automatically when you
open the folder, and it gives the agent the lab's shared context:

- **`SKILL.md`** — the object model (exact names, columns, grain), the build workflow, and
  the checkpoints. This is why "flatten the payload into typed columns" resolves to the same
  `SILVER_LOGS` for everyone.
- **`references/object_model.md`** — the canonical DDL for each layer.
- **`references/agent_spec.md`** and **`references/dashboard_spec.md`** — the agent grounding
  and the dashboard panels.

The skill doesn't do the thinking — your prompt still expresses the intent. It keeps
everyone's objects identical so each step lines up with the next.

### Build your own skill

This is the reusable takeaway: encode *your* team's conventions once, and every prompt after
follows them. A skill is just a folder with a `SKILL.md` (YAML front matter with a `name` and
`description`, then a markdown body of instructions):

```
.snowflake/cortex/skills/my-team/
  SKILL.md            (required)
  references/         (optional supporting docs)
```

Put your naming standards, object model, and DDL patterns in `SKILL.md`, commit it with your
repo, and it auto-loads for anyone who opens the project in CoCo Desktop — or register and
publish it from **Agent Settings → Skills** to share across your team (local folder, Git
repo, Snowflake stage, or the Skills Catalog). Use this repo's `coco-streaming-vhol` skill as
a working template.

## The Log Data Producer (reference)

Emits realistic per-service Snowmart logs and streams them into `BRONZE_LOGS`. CoCo
starts it for you in Parts 1 and 7. These are the underlying commands, run with the
venv's Python so it finds the Snowpipe Streaming SDK. Use the interpreter path for your
OS: `.venv/bin/python` on macOS/Linux, `.venv\Scripts\python.exe` on Windows (the lines
below show the macOS/Linux path).

```
# healthy
.venv/bin/python producer/log_data_producer.py --profile producer/profile.json --rps 200

# trigger the incident (payment-service cascade)
.venv/bin/python producer/log_data_producer.py --profile producer/profile.json --rps 200 \
    --fault checkout_cascade --fault-after 30

# test the data locally without Snowflake
.venv/bin/python producer/log_data_producer.py --dry-run --fault checkout_cascade --fault-after 10
```

On Windows, replace `.venv/bin/python` with `.venv\Scripts\python.exe` and put the
whole command on one line (no trailing `\`).

## Cleanup

Run `solutions/09_cleanup.sql`. Part A drops the lab objects (database, warehouse) and is
safe to run from CoCo. Part B (dropping `VHOLuser` and its network policy) is optional and
commented out: CoCo is connected **as** `VHOLuser`, so don't drop it from that connection —
run those lines from a Snowsight worksheet signed in as your trial's own admin user, or just
let the trial expire.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](./LICENSE).

## Disclaimer

This repository contains sample code provided for educational and demonstration
purposes only. It is provided **"AS IS"**, without warranty of any kind, express or
implied, including but not limited to the warranties of merchantability, fitness for a
particular purpose, and noninfringement. This is **not** official Snowflake product
documentation and is **not** covered by any Snowflake support agreement or SLA.
Snowflake assumes no liability for any damages arising from the use of this code. Review
and test all code in a non-production environment before use, and follow your
organization's security and cost-governance practices. Running these examples consumes
Snowflake credits, which are your responsibility.

&copy; 2026 Snowflake Inc. All rights reserved.
