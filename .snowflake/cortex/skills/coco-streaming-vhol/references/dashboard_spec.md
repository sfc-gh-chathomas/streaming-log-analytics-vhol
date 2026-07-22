# Dashboard Spec: Real-Time SRE Dashboard

A Streamlit-in-Snowflake app with three panels that make the latency story visible.
The point: the aggregates are ~1 minute fresh by design, but the raw layer lands in
~5 seconds, so we show both and label the difference.

## Panels

1. **Per-layer freshness (headline).** One query unions the current lag of each layer
   so the room watches Bronze sit at ~seconds while Silver/Gold/Serving hover near the
   1-minute Dynamic Table target lag. **Measure Bronze from the event time in the payload
   (`PAYLOAD:ts`), NOT a `LANDED_TS` column default** — Snowpipe Streaming does not
   reliably apply `DEFAULT CURRENT_TIMESTAMP()` to streamed rows, so a landed-time default
   lags behind reality and shows huge, wrong Bronze latency. Event time also gives the true
   produce->queryable latency. Parse it TZ-safely with `TO_TIMESTAMP_TZ(PAYLOAD:ts::string)`:
   ```sql
   SELECT 'bronze'  AS layer, DATEDIFF('second', MAX(TO_TIMESTAMP_TZ(PAYLOAD:ts::string)), CURRENT_TIMESTAMP()) AS lag_s FROM BRONZE_LOGS
   UNION ALL SELECT 'silver',  DATEDIFF('second', MAX(event_ts), CURRENT_TIMESTAMP()) FROM SILVER_LOGS
   UNION ALL SELECT 'gold',    DATEDIFF('second', MAX(minute_bucket), CURRENT_TIMESTAMP()) FROM GOLD_SERVICE_HEALTH
   UNION ALL SELECT 'serving', DATEDIFF('second', MAX(minute_bucket), CURRENT_TIMESTAMP()) FROM SERVICE_HEALTH_SERVING
   ```
   (Requires the account timezone to be UTC, which the bootstrap sets, so event-time math
   lines up with `CURRENT_TIMESTAMP()`.)
2. **Live raw feed.** Newest ~15 rows from `BRONZE_LOGS` with a `seconds_ago` column
   (also from `PAYLOAD:ts`, not `LANDED_TS`). Give it a roomy, tall panel — this is the
   visceral "it's live" moment and should not be cramped into a narrow column.
3. **Service health (Gold).** From `SERVICE_HEALTH_SERVING` (the Interactive Table): a
   worst-service KPI row, a **bar chart of current error rate by service** (clean snapshot),
   and a **single line chart of error rate over time for the worst service** (the incident
   spike). Do NOT plot every service as its own line on one chart — with 6-8 services that
   is an unreadable tangle. One snapshot bar chart + one focused trend line reads far better.
4. **Silver stats.** A small metrics row that shows what the Silver Dynamic Table does to
   the raw stream over a recent window (last ~10 min): **Raw events (Bronze)**, **Heartbeats
   filtered**, **Duplicates removed** (raw − heartbeats − clean, floored at 0), **Clean
   events (Silver)**. This makes Silver visible and tells its three-jobs story (flatten,
   filter HEARTBEATs, dedupe).

## Layout

Organize the body top-to-bottom by medallion layer, most-refined first, each with a
section header, so the pipeline reads Gold -> Silver -> Bronze:

- Headline freshness metrics row across the top (4 metrics: bronze, silver, gold, serving),
  with a caption noting Bronze should be the freshest and that if it is not, the producer
  has stopped.
- **Gold — service health**: worst-service KPIs, then the current-error-rate bar chart and
  worst-service trend line side by side.
- **Silver — cleaned & deduplicated**: the 4 Silver-stats metrics.
- **Bronze — raw live feed**: the roomy, tall `st.dataframe` of the newest rows.

Separate sections with `st.markdown("---")` (not `st.divider()`, which is 1.23+).

## Design rules

- **Query `BRONZE_LOGS` directly** for the raw feed and freshness. Do not route Bronze
  through an Interactive Table — that adds the 1-minute refresh lag and defeats the
  low-latency point. Bronze is already the seconds-fresh surface.
- Use `snowflake.snowpark.context.get_active_session()` (Streamlit in Snowflake).
- Short auto-refresh (~5s) with a `@st.cache_data(ttl=3)` guard.
- The Interactive Table stays the serving layer for the aggregates (fast, concurrent).

## Streamlit-in-Snowflake compatibility (avoid version pitfalls)

Streamlit in Snowflake runs an older Streamlit than local, so newer-only APIs fail at
runtime. Generate version-safe code:

- **No `hide_index`** on `st.dataframe(...)` (added in Streamlit 1.23 and not present in
  SiS; it raises `TypeError: ... got an unexpected keyword argument 'hide_index'`). Use
  `st.dataframe(df, use_container_width=True)` instead.
- **No `st.divider()`** (also 1.23+). Use `st.markdown("---")` for a separator.
- **Auto-refresh in place with a fragment, NOT a full-page rerun.** Put all the live panels in
  one function and refresh only that function on a timer, so the charts and tables update in
  place. A whole-page `st.rerun()` (or a meta refresh) re-lays-out the entire app every tick:
  it flashes a green "Running" status and makes the charts and tables **bounce/reflow**, which
  looks broken during a demo. Prefer `st.fragment(run_every=refresh_s)`; it reruns only the
  fragment, leaving the title and sidebar static and skipping the full-page reflow. Resolve it
  defensively so it also works on older SiS runtimes (`experimental_fragment`), and fall back
  to a sleep + rerun loop only if no fragment API exists:
  ```python
  def render_dashboard():
      ...  # freshness metrics + Gold + Silver + Bronze panels

  _fragment = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", None)
  if _fragment:
      _fragment(run_every=refresh_s)(render_dashboard)()   # in-place refresh, no bounce
  else:
      render_dashboard()
      time.sleep(refresh_s)
      _rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
      if _rerun:
          _rerun()
  ```
  Do NOT use an HTML meta refresh (`<meta http-equiv="refresh">`) and do NOT use `st.autorefresh`
  (not a real API). Keep the title, sidebar slider, and `st.set_page_config` OUTSIDE the
  fragment so only the data panels update.

## Prompt to build it

> Build a Streamlit dashboard for the streaming VHOL with three panels: (1) a per-layer
> freshness meter showing the current lag of BRONZE_LOGS, SILVER_LOGS,
> GOLD_SERVICE_HEALTH, and SERVICE_HEALTH_SERVING; (2) a live raw log feed querying
> BRONZE_LOGS directly with how many seconds ago each row landed; (3) error rate and
> p95 latency per service from SERVICE_HEALTH_SERVING with the worst service
> highlighted. Auto-refresh every 5 seconds.

The reference implementation is in the repo at `dashboard/streamlit_app.py`.

## Deploy (no Snowflake CLI)

This is a Streamlit in Snowflake app: there is no `snow` CLI step and no local server. Write
`dashboard/streamlit_app.py`, then deploy it inside the account one of two ways:

- **SQL over the connection (what CoCo can do directly):** `CREATE STAGE` in
  `STREAMING_HOL.LOGS`, `PUT file://.../dashboard/streamlit_app.py @<stage>` (the Snowflake
  Python connector supports PUT, so this works over CoCo's SQL connection), then
  `CREATE OR REPLACE STREAMLIT STREAMING_HOL.LOGS.<name> FROM '@<stage>'
  MAIN_FILE='streamlit_app.py' QUERY_WAREHOUSE=HOL_WH`. Note the syntax is `FROM '@stage'`
  with `MAIN_FILE`, not `ROOT_LOCATION`.
- **Snowsight UI:** Projects -> Streamlit -> **+ Streamlit App**, choose `STREAMING_HOL.LOGS`
  and warehouse `HOL_WH`, and paste the file contents.

The app reads the tables directly via `snowflake.snowpark.context.get_active_session()`, so it
needs no connection profile or credentials.
