# `logs.raw` event schema

> **All data in this lab is synthetic.** Every service name, host, namespace,
> logger, message, trace id, error code and metric is invented for this
> repository. Nothing here is captured from, derived from, or traceable to any
> real system, cluster, tenant, or person. The generator has no data source —
> it fabricates every field from static lists and a random number generator.

One record per Kafka message, UTF-8 encoded JSON, no envelope, no newline
framing. The Kafka message key is the `service` value (or `null` when the
record is a deliberately malformed one that dropped `service`).

## Required fields

| Field        | Type          | Notes |
|--------------|---------------|-------|
| `ts`         | string        | ISO-8601 UTC, millisecond precision, `Z` suffix. Example `2026-09-07T15:04:05.123Z`. |
| `service`    | string        | Logical service emitting the record. One of `checkout-api`, `inventory-svc`, `user-api`, `edge-gateway`, `search-svc`, `billing-worker`, `notify-svc`. |
| `host`       | string        | Instance identity, always `<service>-<n>`, e.g. `checkout-api-3`. Not a resolvable hostname. |
| `level`      | string        | One of `DEBUG`, `INFO`, `WARN`, `ERROR`. Roughly 25 / 55 / 13 / 7 percent. |
| `logger`     | string        | Dotted logger name scoped to the service, e.g. `payment.client`, `queue.consumer`. |
| `message`    | string        | Short fixed phrase, e.g. `request completed`, `cache miss`, `payment provider timeout`. Never interpolated with variable data. |
| `trace_id`   | string        | 32 lowercase hex characters, W3C trace-context style. Randomly generated per event, not correlated across events. |
| `error_code` | string / null | Set only when `level` is `ERROR`, otherwise `null`. Generic codes such as `PAY_TIMEOUT`, `DB_POOL_EXHAUSTED`. |
| `latency_ms` | integer       | Synthetic duration in milliseconds. Skewed higher for `WARN` and `ERROR`. |

## Optional fields

| Field         | Type    | Notes |
|---------------|---------|-------|
| `namespace`   | string  | Always present. One of `shop`, `identity`, `platform`, `data`. |
| `path`        | string  | Present only for HTTP-facing services. Generic paths such as `/checkout`, `/search`, `/health`. |
| `http_status` | integer | Present whenever `path` is present. Correlated with `level` (2xx for `DEBUG`/`INFO`, 4xx for `WARN`, 5xx for `ERROR`). `/health` reports `200` unless the level is `WARN` or `ERROR`. |

Background workers (`billing-worker`, `notify-svc`) emit no `path` and no
`http_status`.

## Deliberately invalid records

About 5 percent of records (`--invalid-ratio`, default `0.05`) are emitted
malformed on purpose so a later NiFi dead-letter route has something to send to
`logs.dlq`. Consumers of `logs.raw` must not assume every record parses.

| Mode              | What is wrong |
|-------------------|---------------|
| `truncated_json`  | The serialized JSON is cut at a random offset, so it does not parse. |
| `missing_service` | Valid JSON, but the required `service` field is absent. Message key is `null`. |
| `missing_level`   | Valid JSON, but the required `level` field is absent. |

A downstream validator should treat a record as good only if it parses as a
JSON object **and** carries all nine required fields with `level` in the
allowed set.

## What is never generated

No email addresses, card numbers, passwords, API tokens, JWTs, cookies,
session ids, customer or account identifiers, IP addresses, internal or
routable URLs, real hostnames, real namespaces, real cluster names, vendor
product names, or stack traces from any real system.
