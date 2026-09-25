# nifi-kafka-log-lab

A local lab: a Python generator makes fake service logs, Kafka carries them, and
NiFi validates, enriches and routes them. All data is synthetic.

```
generator -> logs.raw -> NiFi -> logs.enriched
                                 logs.alerts
                                 logs.dlq.invalid
                                 logs.dlq.unmatched
```

## Start

```bash
docker compose up -d
```

This starts Kafka and NiFi, and runs `kafka-init` once to create the topics.
NiFi needs 1–2 minutes; check with `docker compose ps`.

Kafka addresses:

- from your machine: `localhost:9092`
- from other containers, including NiFi: `kafka:9093`

## Topics

| Topic | Holds |
|---|---|
| `logs.raw` | Everything the generator sends, good and bad. |
| `logs.enriched` | Valid records, enriched from the service catalog. |
| `logs.alerts` | Enriched records at `WARN` or `ERROR`. |
| `logs.dlq.invalid` | Records that don't parse or fail validation. |
| `logs.dlq.unmatched` | Valid records whose `service` isn't in the catalog. |

Auto-creation is off, so a topic only exists if `kafka-init` created it.

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Generate logs

Install dependencies with [uv](https://docs.astral.sh/uv/), or use
`requirements.txt` with pip:

```bash
uv sync
```

Run it:

```bash
uv run generator/produce_logs.py                        # stream until Ctrl+C
uv run generator/produce_logs.py --count 20 --rate 5    # fixed batch
uv run generator/produce_logs.py --dry-run --count 5    # print, don't send
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--rate` | `3.0` | events per second |
| `--invalid-ratio` | `0.05` | share of records broken on purpose |
| `--count` | unset | stop after N records |
| `--topic` | `logs.raw` | destination topic |
| `--bootstrap-servers` | `localhost:9092` | broker list |
| `--seed` | unset | repeatable runs |
| `--dry-run` | off | print instead of produce |

`KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC` override the last two defaults.

Read a topic:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic logs.raw --from-beginning --timeout-ms 10000
```

One JSON object per message, keyed by `service`:

```json
{"ts":"2026-09-07T16:00:59.058Z","service":"checkout-api","host":"checkout-api-4","level":"INFO","logger":"http.server","message":"request completed","trace_id":"838fda7bb317451e9f99e30b17678ba5","error_code":null,"latency_ms":143,"namespace":"shop","path":"/checkout","http_status":200}
```

`error_code` is set only at `ERROR`. Background services (`billing-worker`,
`notify-svc`) have no `path` or `http_status`. About 5% of records are broken on
purpose — truncated JSON, or missing `service` or `level` — so the DLQ paths have
something to handle.

## NiFi

| | |
|---|---|
| URL | <https://localhost:8443/nifi> |
| User | `admin` |
| Password | `labPassword12345` |

The certificate is self-signed, so accept the browser warning.

The flow is **not** created for you — build it on the canvas:

```
ConsumeKafka(logs.raw) -> ValidateRecord -> LookupRecord -> UpdateRecord -> QueryRecord
                                                              WARN/ERROR -> logs.alerts
                                                              else       -> logs.enriched
  parse failure / invalid ---------------------------------------------> logs.dlq.invalid
  unmatched ------------------------------------------------------------> logs.dlq.unmatched
```

Two things to know:

- Inside NiFi, Kafka is `kafka:9093`. `localhost:9092` points at NiFi itself.
- NiFi 2.x uses `ConsumeKafka` / `PublishKafka` with a `Kafka3ConnectionService`,
  not the old `*KafkaRecord_*` processors. Set ConsumeKafka's Processing
  Strategy to `RECORD` to get record handling and the `parse failure` route.

Exact properties and connections: [`nifi/FLOW.md`](nifi/FLOW.md).

`nifi/service_catalog.csv` is mounted read-only at
`/opt/nifi/nifi-current/conf/service_catalog.csv`. It maps each service to a
team, tier, Slack channel and runbook (all invented) and NiFi uses it for
enrichment.

## Stop

```bash
docker compose down      # keep data
docker compose down -v   # delete Kafka data, NiFi state and your flow
```
