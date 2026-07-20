"""
Snowmart Real-Time SRE Dashboard  (Streamlit in Snowflake)
========================================================
Organized top-to-bottom by medallion layer, most-refined first:

  Gold    — service health: error rate and p95 per service (the incident view).
  Silver  — cleaned & deduplicated: what the Silver Dynamic Table filters and dedupes.
  Bronze  — raw live feed: newest rows straight from BRONZE_LOGS (the "it's live" moment).

A per-layer freshness meter across the top makes the latency story visible: Bronze leads
at ~seconds (Snowpipe Streaming) while the refined layers follow near their 1-minute
Dynamic Table target lag.

Deploy as a Streamlit in Snowflake app in STREAMING_HOL.LOGS.

Freshness note: Snowpipe Streaming does NOT reliably apply a LANDED_TS column DEFAULT to
streamed rows, so we measure freshness and seconds-ago from the event time the producer
stamps in the payload (PAYLOAD:ts, UTC). That is also the true produce->queryable latency.
"""

import time

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

DB = "STREAMING_HOL.LOGS"
BRONZE  = f"{DB}.BRONZE_LOGS"
SILVER  = f"{DB}.SILVER_LOGS"
GOLD    = f"{DB}.GOLD_SERVICE_HEALTH"
SERVING = f"{DB}.SERVICE_HEALTH_SERVING"
LOOKBACK_MIN = 30
WINDOW_MIN = 10

# Event time from the payload (UTC), not a LANDED_TS default. See module docstring.
EVENT_TS = "TO_TIMESTAMP_TZ(PAYLOAD:ts::string)"

st.set_page_config(page_title="Snowmart SRE Dashboard", layout="wide")
st.title("Snowmart service health — real time")

session = get_active_session()

refresh_s = st.sidebar.slider("Auto-refresh (seconds)", 3, 60, 5)


def q(sql):
    df = session.sql(sql).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df


@st.cache_data(ttl=3)
def freshness():
    return q(f"""
        SELECT 'bronze'  AS layer, 1 AS ord, DATEDIFF('second', MAX({EVENT_TS}),     CURRENT_TIMESTAMP()) AS lag_s FROM {BRONZE}
        UNION ALL SELECT 'silver',  2, DATEDIFF('second', MAX(event_ts),     CURRENT_TIMESTAMP()) FROM {SILVER}
        UNION ALL SELECT 'gold',    3, DATEDIFF('second', MAX(minute_bucket), CURRENT_TIMESTAMP()) FROM {GOLD}
        UNION ALL SELECT 'serving', 4, DATEDIFF('second', MAX(minute_bucket), CURRENT_TIMESTAMP()) FROM {SERVING}
        ORDER BY ord
    """)


@st.cache_data(ttl=3)
def health():
    return q(f"""
        SELECT service, minute_bucket, request_count, error_count, error_rate, p95_latency_ms
        FROM {SERVING}
        WHERE minute_bucket >= DATEADD('minute', -{LOOKBACK_MIN}, CURRENT_TIMESTAMP())
        ORDER BY minute_bucket
    """)


@st.cache_data(ttl=3)
def silver_stats():
    # Show Silver's three jobs over a recent window: flatten, filter HEARTBEATs, dedupe.
    return q(f"""
        WITH b AS (
            SELECT COUNT(*)                                          AS raw_rows,
                   COUNT_IF(PAYLOAD:level::string = 'HEARTBEAT')     AS heartbeats
            FROM {BRONZE}
            WHERE {EVENT_TS} > DATEADD('minute', -{WINDOW_MIN}, CURRENT_TIMESTAMP())
        ),
        s AS (
            SELECT COUNT(*) AS clean_rows
            FROM {SILVER}
            WHERE event_ts > DATEADD('minute', -{WINDOW_MIN}, CURRENT_TIMESTAMP())
        )
        SELECT b.raw_rows, b.heartbeats, s.clean_rows FROM b, s
    """)


@st.cache_data(ttl=3)
def raw_feed():
    return q(f"""
        SELECT PAYLOAD:service::string   AS service,
               PAYLOAD:level::string     AS level,
               PAYLOAD:status_code::int  AS status,
               PAYLOAD:message::string   AS message,
               DATEDIFF('second', {EVENT_TS}, CURRENT_TIMESTAMP()) AS seconds_ago
        FROM {BRONZE}
        ORDER BY {EVENT_TS} DESC
        LIMIT 15
    """)


# ============================ Headline: freshness ============================
st.subheader("Latency by layer — how fresh is each stage right now?")
fresh = freshness()
cols = st.columns(len(fresh))
for col, (_, r) in zip(cols, fresh.iterrows()):
    lag = r["lag_s"]
    val = "—" if pd.isna(lag) else f"{int(lag)}s"
    col.metric(r["layer"].capitalize(), val)
st.caption("Bronze leads at ~seconds (Snowpipe Streaming). Silver/Gold/Serving follow near "
           "their 1-minute Dynamic Table target lag. If Bronze is NOT the freshest layer, "
           "the producer has stopped.")
st.markdown("---")

# ============================ GOLD: service health ===========================
st.header("Gold — service health")
df = health()
if df.empty:
    st.info("Give the Dynamic Tables a minute to refresh.")
else:
    latest_ts = df["minute_bucket"].max()
    latest = df[df["minute_bucket"] == latest_ts].sort_values(
        ["error_rate", "p95_latency_ms"], ascending=False
    )
    worst = latest.iloc[0]
    k1, k2, k3 = st.columns(3)
    k1.metric("Worst service", worst["service"])
    k2.metric("Error rate", f"{worst['error_rate'] * 100:.1f}%")
    k3.metric("p95 latency", f"{int(worst['p95_latency_ms'])} ms")

    c1, c2 = st.columns(2)
    with c1:
        st.caption("Current error rate by service (%)")
        current = latest[["service", "error_rate"]].copy()
        current["error_rate"] = (current["error_rate"] * 100).round(1)
        st.bar_chart(current.set_index("service")["error_rate"], height=260)
    with c2:
        worst_name = worst["service"]
        st.caption(f"Error rate over time — {worst_name}")
        trend = (
            df[df["service"] == worst_name][["minute_bucket", "error_rate"]]
            .set_index("minute_bucket")
        )
        st.line_chart(trend["error_rate"], height=260)

st.markdown("---")

# ============================ SILVER: cleaned & deduped ======================
st.header("Silver — cleaned & deduplicated")
st.caption(f"What the Silver Dynamic Table does to the raw stream (last {WINDOW_MIN} min): "
           "flatten to typed columns, filter HEARTBEATs, dedupe on event_id.")
sv = silver_stats()
if sv.empty or pd.isna(sv.iloc[0]["raw_rows"]):
    st.info("Waiting for data...")
else:
    row = sv.iloc[0]
    raw_rows = int(row["raw_rows"] or 0)
    heartbeats = int(row["heartbeats"] or 0)
    clean_rows = int(row["clean_rows"] or 0)
    dedupe_removed = max(raw_rows - heartbeats - clean_rows, 0)
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Raw events (Bronze)", f"{raw_rows:,}")
    s2.metric("Heartbeats filtered", f"{heartbeats:,}")
    s3.metric("Duplicates removed", f"{dedupe_removed:,}")
    s4.metric("Clean events (Silver)", f"{clean_rows:,}")

st.markdown("---")

# ============================ BRONZE: raw live feed ==========================
st.header("Bronze — raw live feed")
feed = raw_feed()
if feed.empty:
    st.info("Waiting for the producer...")
else:
    st.dataframe(feed, height=420, use_container_width=True)

st.caption(f"Bronze (raw, ~seconds) · Serving over Gold (~1 min) · refresh {refresh_s}s")

# Auto-refresh without a full-page reload: render everything, wait, then re-run the
# script server-side. Updates panels in place (small "Running" indicator) instead of the
# browser reload + "Please wait" clear a meta refresh causes. st.rerun is newer; fall back
# to st.experimental_rerun on older SiS Streamlit runtimes.
time.sleep(refresh_s)
_rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
if _rerun:
    _rerun()
