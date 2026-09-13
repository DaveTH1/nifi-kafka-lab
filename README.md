# nifi-kafka-log-lab

Local single-broker Apache Kafka (KRaft, no ZooKeeper) for log-pipeline experiments.

- From the host: `localhost:9092`
- From other containers on the `lab` network: `kafka:9093`

## Topics

Created by the one-shot `kafka-init` service. **Broker auto-creation is disabled**
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`), so every topic is created explicitly.
All are `--partitions 1 --replication-factor 1`.

| Topic | Purpose |
|---|---|
| `logs.raw` | Ingest. Everything the generator produces, valid and malformed alike. |
| `logs.enriched` | Valid records that were enriched from the service catalog. |
| `logs.alerts` | Enriched records at level `WARN` or `ERROR`. |
| `logs.dlq.invalid` | Records rejected for being malformed or schema-invalid (unparseable JSON, missing required fields). |
| `logs.dlq.unmatched` | Well-formed records whose `service` had no entry in the service catalog. |

> **Renamed:** this lab previously used a single `logs.dlq` topic. It is now split
> into `logs.dlq.invalid` and `logs.dlq.unmatched` so that "bad data" and "unknown
> service" are separate failure modes. `kafka-init` no longer creates `logs.dlq`.

## Start

```bash
docker compose up -d
```

`kafka-init` runs once after the broker reports healthy, creates the topics, and exits.

## List topics

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

To confirm the bootstrap ran cleanly:

```bash
docker compose logs kafka-init
```

### Upgrading an existing lab volume

Topic creation is `--if-not-exists`, so an existing `kafka-data` volume is left
alone — but it will **not** have the two new topics, and it will still contain the
old `logs.dlq`. Create the new topics by hand:

```bash
for t in logs.dlq.invalid logs.dlq.unmatched; do
  docker compose exec kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done
```

Or just re-run the init service, which is idempotent:

```bash
docker compose up kafka-init
```

> **Renaming does not migrate data.** Messages already in `logs.dlq` stay there;
> they are not copied into the new topics. The old topic is harmless if left in
> place. To start completely clean instead, `docker compose down -v` will delete
> the Kafka volume **and all messages in every topic**.

## Stop

```bash
docker compose down      # stop, keep topic data
docker compose down -v   # stop and delete the Kafka volume
```

## Step 2 — synthetic log generator

`generator/produce_logs.py` publishes fabricated JSON service logs to `logs.raw`.
All data is synthetic; see [SCHEMA.md](SCHEMA.md) for the field contract.

### Install

Dependencies are managed with [uv](https://docs.astral.sh/uv/). `uv` provisions
the Python version from `.python-version` and installs the locked dependencies
from `uv.lock`:

```bash
uv sync
```

No manual virtualenv activation is needed — `uv run` uses the project
environment automatically. If you would rather not use `uv`, `requirements.txt`
is kept in sync as a pip fallback.

### Run

```bash
# stream until Ctrl+C, ~3 events/sec, ~5% deliberately invalid
uv run generator/produce_logs.py

# a fixed batch
uv run generator/produce_logs.py --count 20 --rate 5

# preview without touching Kafka
uv run generator/produce_logs.py --dry-run --count 5
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--rate` | `3.0` | target events per second |
| `--invalid-ratio` | `0.05` | fraction of records emitted malformed on purpose |
| `--count` | unset | stop after N records; omit to run until Ctrl+C |
| `--topic` | `logs.raw` | destination topic |
| `--bootstrap-servers` | `localhost:9092` | broker list |
| `--seed` | unset | seed the RNG for repeatable runs |
| `--dry-run` | off | print to stdout instead of producing |

`KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC` override the defaults for
`--bootstrap-servers` and `--topic`.

### Example valid event

```json
{"ts":"2026-09-07T16:00:59.058Z","service":"checkout-api","host":"checkout-api-4","level":"INFO","logger":"http.server","message":"request completed","trace_id":"838fda7bb317451e9f99e30b17678ba5","error_code":null,"latency_ms":143,"namespace":"shop","path":"/checkout","http_status":200}
```

### Example invalid event

About 5% of records are broken on purpose so a later NiFi dead-letter route has
something to send to `logs.dlq`. This one is truncated mid-JSON:

```text
{"ts":"2026-09-07T16:00:58.503Z","service":"edge-gateway","host":"edge-gateway-5","level":"WARN","logger":"proxy.upstream","message":"retrying downstream call","trace_id":"c1d66a
```

The other two failure modes are valid JSON with the required `service` or
`level` field removed.

### Consume `logs.raw`

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic logs.raw --from-beginning --timeout-ms 10000
```

## Step 3 — NiFi

`docker compose up -d` also starts Apache NiFi (pinned to `apache/nifi:2.11.0`).

### Log in

| | |
|---|---|
| URL | <https://localhost:8443/nifi> |
| Username | `admin` |
| Password | `labPassword12345` |

NiFi uses a self-signed certificate, so the browser will warn on first visit —
accept it and continue. NiFi takes roughly 1–2 minutes to become available after
`docker compose up`; wait for the container to report healthy:

```bash
docker compose ps
```

### Service catalog CSV

`nifi/service_catalog.csv` is mounted read-only into the container at:

```
/opt/nifi/nifi-current/conf/service_catalog.csv
```

It maps each generic service the generator emits to a team, tier, Slack channel
and runbook URL. The values are invented for this lab (`example.com` runbooks,
`#lab-*` channels). The `LookupRecord` processor reads it through a
`CSVRecordLookupService` keyed on the `service` column.

### Building the flow

**The flow is not provisioned automatically — you build it by hand in the UI.**
Follow [`nifi/FLOW.md`](nifi/FLOW.md), which lists every processor, the exact
properties, and the connections:

```
ConsumeKafka(logs.raw) -> ValidateRecord -> LookupRecord -> UpdateRecord -> QueryRecord
                                                                             ├─ WARN/ERROR -> logs.alerts
                                                                             └─ else       -> logs.enriched
        parse failure / invalid ------------------------------------------------> logs.dlq.invalid
        unmatched -------------------------------------------------------------> logs.dlq.unmatched
```

> `FLOW.md` still documents a single `PublishDlq` processor writing to `logs.dlq`.
> That topic is no longer created. Splitting the DLQ path into two publishers is a
> NiFi flow change and has not been made yet — see the note in `nifi/FLOW.md`.

Inside NiFi, Kafka is reached at **`kafka:9093`** (the Docker listener), not
`localhost:9092`. NiFi runtime state lives in named Docker volumes, so nothing
from the flow is written into the repository.

Note that NiFi 2.x no longer ships `ConsumeKafkaRecord_*` / `PublishKafkaRecord_*`.
Those were consolidated into `ConsumeKafka` / `PublishKafka` plus a
`Kafka3ConnectionService`; `FLOW.md` uses the current processors.
