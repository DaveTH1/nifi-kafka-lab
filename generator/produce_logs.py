#!/usr/bin/env python3
"""Synthetic service-log generator for the nifi-kafka-log-lab.

Publishes JSON application logs to the Kafka topic ``logs.raw``.

Every value produced here is fabricated. There is no connection to any real
system, cluster, tenant or person. See README.md for the field contract.
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover - only hit when deps are missing
    Producer = None


DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "logs.raw")

LEVELS = ("DEBUG", "INFO", "WARN", "ERROR")
LEVEL_WEIGHTS = (25, 55, 13, 7)

INVALID_MODES = ("truncated_json", "missing_service", "missing_level")

# Fully invented service catalogue. Names, namespaces, loggers, paths, messages
# and error codes below are generic placeholders chosen for this lab.
SERVICES = (
    {
        "service": "checkout-api",
        "namespace": "shop",
        "replicas": 4,
        "loggers": ("payment.client", "http.server", "cart.session"),
        "paths": ("/checkout", "/checkout/confirm", "/health"),
        "errors": ("PAY_TIMEOUT", "CART_LOCKED"),
    },
    {
        "service": "inventory-svc",
        "namespace": "shop",
        "replicas": 3,
        "loggers": ("stock.reader", "db.pool", "http.server"),
        "paths": ("/stock", "/stock/reserve", "/health"),
        "errors": ("STOCK_CONFLICT", "DB_POOL_EXHAUSTED"),
    },
    {
        "service": "user-api",
        "namespace": "identity",
        "replicas": 3,
        "loggers": ("account.store", "http.server", "session.cache"),
        "paths": ("/users", "/users/profile", "/health"),
        "errors": ("USER_NOT_FOUND", "RATE_LIMITED"),
    },
    {
        "service": "edge-gateway",
        "namespace": "platform",
        "replicas": 5,
        "loggers": ("router.match", "proxy.upstream", "tls.handshake"),
        "paths": ("/", "/api", "/health"),
        "errors": ("UPSTREAM_UNAVAILABLE", "ROUTE_NOT_FOUND"),
    },
    {
        "service": "search-svc",
        "namespace": "data",
        "replicas": 3,
        "loggers": ("index.query", "index.writer", "http.server"),
        "paths": ("/search", "/search/suggest", "/health"),
        "errors": ("QUERY_TIMEOUT", "INDEX_STALE"),
    },
    {
        "service": "billing-worker",
        "namespace": "shop",
        "replicas": 2,
        "loggers": ("invoice.job", "queue.consumer", "ledger.writer"),
        "paths": (None,),
        "errors": ("INVOICE_RETRY", "LEDGER_CONFLICT"),
    },
    {
        "service": "notify-svc",
        "namespace": "platform",
        "replicas": 2,
        "loggers": ("queue.consumer", "template.render", "delivery.client"),
        "paths": (None,),
        "errors": ("DELIVERY_BOUNCED", "TEMPLATE_MISSING"),
    },
)

MESSAGES = {
    "DEBUG": (
        "cache miss",
        "cache hit",
        "connection acquired from pool",
        "config reloaded",
        "span started",
        "batch flushed",
    ),
    "INFO": (
        "request completed",
        "job finished",
        "record persisted",
        "worker started",
        "message consumed",
        "health probe ok",
    ),
    "WARN": (
        "retrying downstream call",
        "slow query detected",
        "connection pool near limit",
        "queue backlog growing",
        "response truncated",
    ),
    "ERROR": (
        "payment provider timeout",
        "downstream call failed",
        "failed to persist record",
        "upstream returned error",
        "job aborted after retries",
    ),
}

HTTP_STATUS = {
    "DEBUG": (200, 200, 204),
    "INFO": (200, 200, 201, 204),
    "WARN": (400, 404, 429),
    "ERROR": (500, 502, 503, 504),
}


def _now_iso():
    """Timezone-aware ISO-8601 timestamp in UTC with millisecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _trace_id(rnd):
    """32-char lowercase hex trace id, W3C trace-context style."""
    return uuid.UUID(int=rnd.getrandbits(128), version=4).hex


def _latency_ms(rnd, level):
    if level == "ERROR":
        return rnd.randint(500, 9000)
    if level == "WARN":
        return rnd.randint(200, 2500)
    if level == "DEBUG":
        return rnd.randint(1, 40)
    return rnd.randint(5, 400)


def make_event(rnd):
    """Build one well-formed synthetic log event."""
    svc = rnd.choice(SERVICES)
    level = rnd.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
    path = rnd.choice(svc["paths"])

    event = {
        "ts": _now_iso(),
        "service": svc["service"],
        "host": "{0}-{1}".format(svc["service"], rnd.randint(1, svc["replicas"])),
        "level": level,
        "logger": rnd.choice(svc["loggers"]),
        "message": rnd.choice(MESSAGES[level]),
        "trace_id": _trace_id(rnd),
        "error_code": rnd.choice(svc["errors"]) if level == "ERROR" else None,
        "latency_ms": _latency_ms(rnd, level),
        "namespace": svc["namespace"],
    }
    if path is not None:
        event["path"] = path
        # Probe endpoints only ever report success unless the level says otherwise.
        if path == "/health" and level in ("DEBUG", "INFO"):
            event["http_status"] = 200
        else:
            event["http_status"] = rnd.choice(HTTP_STATUS[level])
    return event


def make_invalid_payload(rnd):
    """Return ``(payload_bytes, key, mode)`` for a deliberately broken record.

    These exist so a later NiFi dead-letter path has something to route to
    ``logs.dlq``. They are malformed on purpose, not by accident.
    """
    mode = rnd.choice(INVALID_MODES)
    event = make_event(rnd)
    key = event["service"]

    if mode == "truncated_json":
        encoded = json.dumps(event, separators=(",", ":"))
        cut = rnd.randint(len(encoded) // 3, len(encoded) - 2)
        return encoded[:cut].encode("utf-8"), key, mode

    if mode == "missing_service":
        event.pop("service", None)
        key = None
    elif mode == "missing_level":
        event.pop("level", None)

    return json.dumps(event, separators=(",", ":")).encode("utf-8"), key, mode


def _delivery_error(err, _msg):
    if err is not None:
        print("delivery failed: {0}".format(err), file=sys.stderr)


def build_producer(bootstrap_servers):
    if Producer is None:
        raise SystemExit(
            "confluent-kafka is not installed. Run: pip install -r requirements.txt"
        )
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "synthetic-log-generator",
            "linger.ms": 50,
            "acks": "all",
            "enable.idempotence": True,
        }
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Publish synthetic JSON service logs to a Kafka topic.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=3.0,
        help="target events per second (default: 3.0)",
    )
    parser.add_argument(
        "--invalid-ratio",
        type=float,
        default=0.05,
        help="fraction of records emitted malformed on purpose (default: 0.05)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="number of records to send, then exit. Omit to run until Ctrl+C.",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="destination topic (default: {0})".format(DEFAULT_TOPIC),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP,
        help="Kafka bootstrap servers (default: {0})".format(DEFAULT_BOOTSTRAP),
    )
    parser.add_argument("--seed", type=int, default=None, help="seed the RNG for repeatable runs")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print records to stdout instead of producing to Kafka",
    )
    args = parser.parse_args(argv)

    if args.rate <= 0:
        parser.error("--rate must be greater than 0")
    if not 0.0 <= args.invalid_ratio <= 1.0:
        parser.error("--invalid-ratio must be between 0.0 and 1.0")
    if args.count is not None and args.count < 0:
        parser.error("--count must not be negative")
    return args


def main(argv=None):
    args = parse_args(argv)
    rnd = random.Random(args.seed)

    producer = None if args.dry_run else build_producer(args.bootstrap_servers)
    interval = 1.0 / args.rate

    running = {"flag": True}

    def _stop(_signum, _frame):
        running["flag"] = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    target = "stdout" if args.dry_run else "{0} -> {1}".format(args.bootstrap_servers, args.topic)
    print(
        "producing synthetic logs to {0} at ~{1} ev/s ({2:.0%} invalid){3}".format(
            target,
            args.rate,
            args.invalid_ratio,
            "" if args.count is None else ", count={0}".format(args.count),
        ),
        file=sys.stderr,
    )

    sent = 0
    invalid = 0
    next_tick = time.monotonic()
    try:
        while running["flag"] and (args.count is None or sent < args.count):
            if rnd.random() < args.invalid_ratio:
                payload, key, _mode = make_invalid_payload(rnd)
                invalid += 1
            else:
                event = make_event(rnd)
                payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
                key = event["service"]

            if args.dry_run:
                sys.stdout.write(payload.decode("utf-8", errors="replace") + "\n")
                sys.stdout.flush()
            else:
                try:
                    producer.produce(
                        args.topic,
                        value=payload,
                        key=None if key is None else key.encode("utf-8"),
                        on_delivery=_delivery_error,
                    )
                except BufferError:
                    producer.flush(5)
                    producer.produce(
                        args.topic,
                        value=payload,
                        key=None if key is None else key.encode("utf-8"),
                        on_delivery=_delivery_error,
                    )
                producer.poll(0)

            sent += 1

            # Pace against a fixed schedule so drift does not accumulate, and
            # jitter slightly so the stream does not look metronomic.
            next_tick += interval * rnd.uniform(0.75, 1.25)
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.monotonic()
    finally:
        if producer is not None:
            remaining = producer.flush(15)
            if remaining:
                print("{0} record(s) not delivered".format(remaining), file=sys.stderr)
        print("sent {0} record(s), {1} invalid".format(sent, invalid), file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
