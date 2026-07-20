#!/usr/bin/env python3
"""
Snowmart Log Data Producer
========================

Emits realistic per-service application logs for the streaming VHOL and sends
them to Snowflake via the Snowpipe Streaming Python SDK (High-Performance
Architecture). Each record is a JSON object landed into the VARIANT column
PAYLOAD of the BRONZE_LOGS table.

Two modes matter for the lab:
  * healthy            steady traffic, low error rates
  * checkout_cascade   payment-service degrades and drags checkout with it,
                       ramping over ~75s so the dashboard shows rising lines
                       and the agent has a real root-cause story to tell.

Test without Snowflake:
    python log_data_producer.py --dry-run --fault checkout_cascade --fault-after 15

Stream into Snowflake:
    python log_data_producer.py --profile profile.json --rps 200 \
        --fault checkout_cascade --fault-after 120
"""

import argparse
import json
import math
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Service model
# ---------------------------------------------------------------------------
# median_ms drives a log-normal latency distribution (real tail, not uniform).
# base_error_rate is the healthy fraction of requests that fail (5xx/4xx).
SERVICES = {
    "gateway":                {"weight": 30, "median_ms": 15,  "base_error_rate": 0.003},
    "auth-service":           {"weight": 12, "median_ms": 40,  "base_error_rate": 0.008},
    "search-service":         {"weight": 20, "median_ms": 60,  "base_error_rate": 0.010},
    "cart-service":           {"weight": 14, "median_ms": 35,  "base_error_rate": 0.007},
    "checkout-service":       {"weight": 8,  "median_ms": 120, "base_error_rate": 0.010},
    "payment-service":        {"weight": 8,  "median_ms": 150, "base_error_rate": 0.012},
    "recommendation-service": {"weight": 6,  "median_ms": 80,  "base_error_rate": 0.015},
    "inventory-service":      {"weight": 10, "median_ms": 45,  "base_error_rate": 0.006},
}

LATENCY_SIGMA = 0.5  # log-normal spread -> believable p95/p99 tail

ENDPOINTS = {
    "gateway":                [("GET", "/"), ("GET", "/health"), ("POST", "/api/route")],
    "auth-service":           [("POST", "/api/login"), ("POST", "/api/token/refresh"), ("GET", "/api/session")],
    "search-service":         [("GET", "/api/search"), ("GET", "/api/search/suggest")],
    "cart-service":           [("GET", "/api/cart"), ("POST", "/api/cart/add"), ("DELETE", "/api/cart/item")],
    "checkout-service":       [("POST", "/api/checkout"), ("POST", "/api/checkout/confirm")],
    "payment-service":        [("POST", "/api/payment/charge"), ("POST", "/api/payment/authorize")],
    "recommendation-service": [("GET", "/api/recommendations"), ("GET", "/api/recommendations/similar")],
    "inventory-service":      [("GET", "/api/inventory"), ("POST", "/api/inventory/reserve")],
}

# Per-service error catalog. Each entry can carry a downstream dependency so the
# agent can reason about root cause from the message content alone.
ERROR_CATALOG = {
    "gateway": [
        {"status": 502, "exception": "BadGateway", "message": "upstream connection reset"},
        {"status": 503, "exception": "ServiceUnavailable", "message": "no healthy upstream for route"},
    ],
    "auth-service": [
        {"status": 401, "exception": "InvalidTokenError", "message": "invalid or expired token"},
        {"status": 500, "exception": "KeyRotationError", "message": "token signing key rotation failed"},
    ],
    "search-service": [
        {"status": 500, "exception": "ShardTimeoutError", "message": "elasticsearch shard timeout after 1000ms"},
        {"status": 429, "exception": "RateLimitError", "message": "search rate limit exceeded"},
    ],
    "cart-service": [
        {"status": 500, "exception": "RedisTimeoutError", "message": "redis timeout on cart:read", "dependency": "redis"},
        {"status": 404, "exception": "CartNotFound", "message": "cart not found for session"},
    ],
    "checkout-service": [
        {"status": 503, "exception": "DownstreamError", "message": "downstream payment-service call failed (503)", "dependency": "payment-service"},
        {"status": 500, "exception": "OrderValidationError", "message": "order validation failed: empty cart"},
    ],
    "payment-service": [
        {"status": 504, "exception": "GatewayTimeout", "message": "payment gateway timeout after 3000ms", "dependency": "stripe-gateway"},
        {"status": 503, "exception": "CircuitOpenError", "message": "circuit breaker OPEN for payment-processor", "dependency": "stripe-gateway"},
        {"status": 500, "exception": "DBConnectionError", "message": "connection pool exhausted (max=20)", "dependency": "payments-db"},
    ],
    "recommendation-service": [
        {"status": 500, "exception": "ModelServerError", "message": "model server unavailable"},
        {"status": 503, "exception": "ServiceUnavailable", "message": "feature store unreachable"},
    ],
    "inventory-service": [
        {"status": 500, "exception": "DeadlockError", "message": "inventory DB deadlock detected", "dependency": "inventory-db"},
        {"status": 409, "exception": "ConflictError", "message": "insufficient stock for reservation"},
    ],
}

REGIONS = ["us-west-2", "us-east-1", "eu-west-1"]
USER_POOL = [f"user_{n:05d}" for n in range(1, 2001)]

_SERVICE_NAMES = list(SERVICES.keys())
_SERVICE_WEIGHTS = [SERVICES[s]["weight"] for s in _SERVICE_NAMES]


# ---------------------------------------------------------------------------
# Fault model
# ---------------------------------------------------------------------------
def fault_intensity(fault, elapsed, fault_after, ramp):
    """Return 0..1 fault intensity for the current moment."""
    if fault == "none" or elapsed < fault_after:
        return 0.0
    if ramp <= 0:
        return 1.0
    return min(1.0, (elapsed - fault_after) / ramp)


def service_state(service, fault, intensity):
    """
    Return (error_rate, latency_multiplier, weighted_error_pool) for a service,
    adjusted for the active fault. weighted_error_pool lets the fault bias which
    error types show up so the story stays coherent.
    """
    cfg = SERVICES[service]
    error_rate = cfg["base_error_rate"]
    latency_mult = 1.0
    error_pool = ERROR_CATALOG[service]

    if fault == "checkout_cascade" and intensity > 0:
        if service == "payment-service":
            error_rate = 0.012 + 0.44 * intensity      # up to ~45%
            latency_mult = 1.0 + 19.0 * intensity        # 150ms -> ~3000ms
            # bias toward timeout + circuit breaker
            error_pool = [ERROR_CATALOG[service][0]] * 3 + [ERROR_CATALOG[service][1]] * 2 + [ERROR_CATALOG[service][2]]
        elif service == "checkout-service":
            error_rate = 0.010 + 0.34 * intensity        # up to ~35%
            latency_mult = 1.0 + 8.0 * intensity         # backs up waiting on payment
            error_pool = [ERROR_CATALOG[service][0]] * 4 + [ERROR_CATALOG[service][1]]
        elif service == "cart-service":
            error_rate = 0.007 + 0.09 * intensity        # mild bump to ~10%
            latency_mult = 1.0 + 1.5 * intensity

    return error_rate, latency_mult, error_pool


# ---------------------------------------------------------------------------
# Record generation
# ---------------------------------------------------------------------------
def make_latency(median_ms, mult):
    base = median_ms * math.exp(random.gauss(0.0, LATENCY_SIGMA))
    return int(max(1, base * mult))


def generate_record(fault, intensity):
    service = random.choices(_SERVICE_NAMES, weights=_SERVICE_WEIGHTS, k=1)[0]
    cfg = SERVICES[service]
    error_rate, latency_mult, error_pool = service_state(service, fault, intensity)

    method, endpoint = random.choice(ENDPOINTS[service])
    latency_ms = make_latency(cfg["median_ms"], latency_mult)
    is_error = random.random() < error_rate

    rec = {
        "event_id": uuid.uuid4().hex,
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "trace_id": uuid.uuid4().hex,
        "span_id": uuid.uuid4().hex[:16],
        "pod": f"{service}-{random.randint(0, 5)}",
        "region": random.choice(REGIONS),
        "http_method": method,
        "endpoint": endpoint,
        "user_id": random.choice(USER_POOL),
    }

    if is_error:
        err = random.choice(error_pool)
        rec["status_code"] = err["status"]
        rec["latency_ms"] = latency_ms
        rec["level"] = "ERROR" if err["status"] >= 500 else "WARN"
        rec["exception_type"] = err["exception"]
        rec["message"] = err["message"]
        if "dependency" in err:
            rec["dependency"] = err["dependency"]
    else:
        rec["status_code"] = 200
        rec["latency_ms"] = latency_ms
        # flag genuinely slow-but-successful calls as WARN
        rec["level"] = "WARN" if latency_ms > cfg["median_ms"] * 6 else "INFO"
        rec["message"] = f"{method} {endpoint} 200 in {latency_ms}ms"

    return rec


def maybe_noise(rec):
    """
    Occasionally emit records that the Silver layer is expected to clean:
      * ~1% HEARTBEAT lines (filtered out in Silver)
      * ~0.5% duplicates by event_id (deduplicated in Silver)
    Returns a list of records to send for this tick.
    """
    out = [rec]
    r = random.random()
    if r < 0.010:
        out.append({
            "event_id": uuid.uuid4().hex,
            "ts": datetime.now(timezone.utc).isoformat(),
            "service": rec["service"],
            "level": "HEARTBEAT",
            "status_code": 200,
            "latency_ms": 0,
            "trace_id": uuid.uuid4().hex,
            "span_id": uuid.uuid4().hex[:16],
            "pod": rec["pod"],
            "region": rec["region"],
            "http_method": "GET",
            "endpoint": "/health",
            "user_id": "healthcheck",
            "message": "heartbeat",
        })
    elif r < 0.015:
        out.append(dict(rec))  # exact duplicate (same event_id) -> deduped in Silver
    return out


# ---------------------------------------------------------------------------
# Streaming client (lazy import so --dry-run needs no SDK)
# ---------------------------------------------------------------------------
class Sink:
    def send(self, rec):
        raise NotImplementedError

    def flush(self):
        pass

    def close(self):
        pass


class DryRunSink(Sink):
    def send(self, rec):
        sys.stdout.write(json.dumps(rec) + "\n")


class SnowflakeSink(Sink):
    def __init__(self, args):
        from snowflake.ingest.streaming import StreamingIngestClient

        self.client = StreamingIngestClient(
            client_name="snowmart_log_producer",
            db_name=args.database,
            schema_name=args.schema,
            pipe_name=f"{args.table}-STREAMING",
            profile_json=args.profile,
        )
        self.channel, _ = self.client.open_channel(channel_name="producer_1")
        self._offset = 0

    def send(self, rec):
        self.channel.append_row({"PAYLOAD": rec}, offset_token=str(self._offset))
        self._offset += 1

    def flush(self):
        self.channel.wait_for_flush(timeout_seconds=30)

    def close(self):
        try:
            self.channel.close()
        finally:
            self.client.close()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description="Snowmart log data producer for the streaming VHOL")
    p.add_argument("--rps", type=int, default=200, help="approx events per second")
    p.add_argument("--duration", type=int, default=0, help="seconds to run (0 = forever)")
    p.add_argument("--fault", choices=["none", "checkout_cascade"], default="none")
    p.add_argument("--fault-after", type=int, default=120, help="seconds before fault begins")
    p.add_argument("--fault-ramp", type=int, default=75, help="seconds to ramp fault to full")
    p.add_argument("--dry-run", action="store_true", help="print JSON to stdout, no Snowflake")
    p.add_argument("--profile", default="profile.json", help="Snowpipe Streaming profile JSON path")
    p.add_argument("--database", default="STREAMING_HOL")
    p.add_argument("--schema", default="LOGS")
    p.add_argument("--table", default="BRONZE_LOGS")
    args = p.parse_args()

    sink = DryRunSink() if args.dry_run else SnowflakeSink(args)

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    start = time.time()
    sent = 0
    last_report = start
    err = sys.stderr

    err.write(f"Producer starting: rps={args.rps} fault={args.fault} "
              f"fault_after={args.fault_after}s dry_run={args.dry_run}\n")
    err.flush()

    try:
        while not stop["flag"]:
            tick_start = time.time()
            elapsed = tick_start - start
            if args.duration and elapsed >= args.duration:
                break

            intensity = fault_intensity(args.fault, elapsed, args.fault_after, args.fault_ramp)

            for _ in range(args.rps):
                for rec in maybe_noise(generate_record(args.fault, intensity)):
                    sink.send(rec)
                    sent += 1

            now = time.time()
            if now - last_report >= 5:
                phase = "FAULT" if intensity > 0 else "healthy"
                err.write(f"[{int(elapsed):>4}s] sent={sent} phase={phase} "
                          f"intensity={intensity:.2f}\n")
                err.flush()
                last_report = now
                if not args.dry_run:
                    sink.flush()

            # pace to ~rps per second
            drift = 1.0 - (time.time() - tick_start)
            if drift > 0:
                time.sleep(drift)
    finally:
        err.write(f"Stopping. flushing {sent} sent...\n")
        err.flush()
        sink.flush()
        sink.close()


if __name__ == "__main__":
    main()
