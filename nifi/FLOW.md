# Building the log pipeline in the NiFi UI

This document is the build sheet for the Step 3 flow. Nothing here is
auto-imported: you build the flow by hand on the NiFi canvas, and every value in
the tables below was verified against `apache/nifi:2.11.0` running from this
repository's `docker-compose.yml`.

All data flowing through this pipeline is synthetic (see `../SCHEMA.md`).

> ⚠️ **Out of date with the topic bootstrap.** `kafka-init` no longer creates
> `logs.dlq`; it now creates `logs.dlq.invalid` and `logs.dlq.unmatched`. The
> single `PublishDlq` processor documented below still targets `logs.dlq`, and
> because broker auto-creation is disabled it **will fail** against a freshly
> created lab. Splitting `PublishDlq` into two publishers (`parse failure` +
> `invalid` → `logs.dlq.invalid`, `unmatched` → `logs.dlq.unmatched`) is a NiFi
> flow change that has not been made yet.

## Pipeline

```
ConsumeKafka (logs.raw, RECORD)
  ├─ success ───────────► ValidateRecord
  └─ parse failure ─────► PublishDlq  (logs.dlq)

ValidateRecord (ts, service, level, message required)
  ├─ valid ─────────────► LookupRecord
  └─ invalid, failure ──► PublishDlq  (logs.dlq)

LookupRecord (CSV lookup on /service)
  ├─ matched ───────────► UpdateRecord
  └─ unmatched, failure ► PublishDlq  (logs.dlq)

UpdateRecord (adds enriched_at)
  ├─ success ───────────► QueryRecord
  └─ failure ───────────► PublishDlq  (logs.dlq)

QueryRecord
  ├─ alerts   (WARN/ERROR) ► PublishAlerts   (logs.alerts)
  └─ enriched (everything else) ► PublishEnriched (logs.enriched)
```

## Before you start: the one thing that will bite you

**Kafka processors must use the Docker listener `kafka:9093`.**
`localhost:9092` is the *host* listener. From inside the NiFi container that
address points at NiFi itself and the connection will fail. Use `kafka:9093`
everywhere in NiFi.

Also note that in NiFi 2.x the old `ConsumeKafkaRecord_*` / `PublishKafkaRecord_*`
processors no longer exist. The bundle was consolidated into **`ConsumeKafka`**
and **`PublishKafka`**, which get their broker settings from a
**`Kafka3ConnectionService`** controller service, and do record processing via a
`Processing Strategy` / `Record Reader` / `Record Writer` combination.

## Step 1 — Controller services

Right-click empty canvas → **Configure** → **Controller Services** tab → **+**.
Create all four, then enable them all.

> Do **not** use hamburger → Controller Settings → *Management Controller
> Services*. Those are flow-controller scoped and are only visible to reporting
> tasks — processors will not see them in their dropdowns. Controller services
> used by processors must live in the same process group as those processors, or
> in an ancestor group.

| # | Service type | Name | Property | Value |
|---|--------------|------|----------|-------|
| 1 | `JsonTreeReader` | `JsonReader` | *(all defaults)* | Schema Access Strategy stays `Infer Schema` |
| 2 | `JsonRecordSetWriter` | `JsonWriter` | *(all defaults)* | Schema Write Strategy stays `Do Not Write Schema`, Schema Access Strategy `Inherit Record Schema` |
| 3 | `Kafka3ConnectionService` | `KafkaConn` | **Bootstrap Servers** | `kafka:9093` |
|   | | | **Security Protocol** | `PLAINTEXT` (default is `SSL` — you must change it) |
| 4 | `CSVRecordLookupService` | `SvcCatalog` | **CSV File** | `/opt/nifi/nifi-current/conf/service_catalog.csv` |
|   | | | **Lookup Key Column** | `service` |
|   | | | **CSV Format** | `default` |

Enable each one with the lightning-bolt icon. All four must show **Enabled** and
a valid (no warning triangle) state before you continue.

## Step 2 — Processors

Drag a processor onto the canvas, search for the type, then set the properties
below. Anything not listed stays at its default.

### ConsumeKafka

| Property | Value |
|----------|-------|
| Kafka Connection Service | `KafkaConn` |
| Group ID | `nifi-log-lab` |
| Topic Format | `names` |
| **Topics** | `logs.raw` |
| **Processing Strategy** | `RECORD` |
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |
| Auto Offset Reset | `earliest` |

`Processing Strategy = RECORD` is what turns this into the record-based consumer
(the old `ConsumeKafkaRecord_*` processor no longer exists in NiFi 2.x). In
RECORD mode the processor gains a **`parse failure`** relationship, which is
where messages that are not parseable JSON come out — route it to the DLQ.

### ValidateRecord

| Property | Value |
|----------|-------|
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |
| **Schema Access Strategy** | `Use "Schema Text" Property` |
| **Schema Text** | the Avro schema below |
| Allow Extra Fields | `true` |
| Strict Type Checking | `false` |

```json
{
  "type": "record",
  "name": "logEvent",
  "fields": [
    { "name": "ts",      "type": "string" },
    { "name": "service", "type": "string" },
    { "name": "level",   "type": "string" },
    { "name": "message", "type": "string" }
  ]
}
```

`Allow Extra Fields = true` lets the other fields (`host`, `logger`, `trace_id`,
`latency_ms`, `namespace`, `path`, `http_status`, `error_code`) pass through
untouched while still requiring the four listed above. Records missing any of
them come out on **`invalid`**.

### LookupRecord

| Property | Value |
|----------|-------|
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |
| Lookup Service | `SvcCatalog` |
| **Routing Strategy** | `Route to 'matched' or 'unmatched'` |
| **Record Result Contents** | `Insert Record Fields` |
| Record Update Strategy | `Use Property` |
| **Result RecordPath** | `/` |

Then add one **dynamic property** (the **+** in the top right of the properties
tab). This is the lookup key and it is easy to miss:

| Dynamic property name | Value |
|-----------------------|-------|
| `key` | `/service` |

`key` is the coordinate name that `CSVRecordLookupService` expects; `/service`
is the RecordPath whose value is looked up in the CSV.

`Record Result Contents = Insert Record Fields` with `Result RecordPath = /`
merges `team`, `tier`, `slack_channel` and `runbook` in as **top-level** fields.
If you leave it on the default `Insert Entire Record`, they end up nested under
a `root` object instead.

### UpdateRecord

| Property | Value |
|----------|-------|
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |
| Replacement Value Strategy | `Literal Value` |

Plus one dynamic property:

| Dynamic property name | Value |
|-----------------------|-------|
| `/enriched_at` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "GMT")}` |

### QueryRecord

| Property | Value |
|----------|-------|
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |

Plus two dynamic properties. Each one creates a relationship of the same name:

| Dynamic property name | Value |
|-----------------------|-------|
| `alerts` | `SELECT * FROM FLOWFILE WHERE level IN ('WARN','ERROR')` |
| `enriched` | `SELECT * FROM FLOWFILE WHERE level NOT IN ('WARN','ERROR')` |

In the **Relationships** tab, auto-terminate `original` and `failure`.

### PublishKafka × 3

Create three, one per destination topic.

| Processor name | Topic Name | Record Reader | Record Writer |
|----------------|-----------|---------------|---------------|
| `PublishEnriched` | `logs.enriched` | `JsonReader` | `JsonWriter` |
| `PublishAlerts` | `logs.alerts` | `JsonReader` | `JsonWriter` |
| `PublishDlq` | `logs.dlq` | *(leave empty)* | *(leave empty)* |

All three also get:

| Property | Value |
|----------|-------|
| Kafka Connection Service | `KafkaConn` |
| Transactions Enabled | `false` |

On each, auto-terminate **`success`** and **`failure`** in the Relationships tab.

> `PublishDlq` deliberately has **no** record reader or writer. The DLQ has to
> accept bytes that are not valid JSON, so it must pass the original content
> straight through rather than trying to parse it.

## Step 3 — Connections

Drag from each processor to the next and tick the listed relationships.

| From | Relationship(s) | To |
|------|-----------------|-----|
| ConsumeKafka | `success` | ValidateRecord |
| ConsumeKafka | `parse failure` | PublishDlq |
| ValidateRecord | `valid` | LookupRecord |
| ValidateRecord | `invalid`, `failure` | PublishDlq |
| LookupRecord | `matched` | UpdateRecord |
| LookupRecord | `unmatched`, `failure` | PublishDlq |
| UpdateRecord | `success` | QueryRecord |
| UpdateRecord | `failure` | PublishDlq |
| QueryRecord | `alerts` | PublishAlerts |
| QueryRecord | `enriched` | PublishEnriched |

Until a relationship is either connected or auto-terminated, the processor shows
as invalid. That is expected while you are still wiring things up.

## Step 4 — Run it

Select the canvas background and press **Start** (or right-click → Start) to
start every processor, then generate traffic from the repo root:

```bash
uv run generator/produce_logs.py --count 60 --rate 20
```

Check where the records landed:

```bash
for t in logs.raw logs.enriched logs.alerts logs.dlq; do
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$t"
done
```

The three output counts should add up to the number of records in `logs.raw`.
A verified run of 60 events on NiFi 2.11.0 produced:

```
logs.enriched  +49
logs.alerts    +8
logs.dlq       +3      # the generator's deliberately-invalid records
                       # 49 + 8 + 3 = 60
```

An enriched record looks like this — note the flat `team` / `tier` /
`slack_channel` / `runbook` fields from the CSV, plus `enriched_at`:

```json
{"ts":"2026-09-07T17:15:18.454Z","service":"user-api","host":"user-api-1","level":"DEBUG","logger":"http.server","message":"cache hit","trace_id":"a00886d7777348cca98f1268fbdcf3f0","error_code":null,"latency_ms":19,"namespace":"identity","path":"/users/profile","http_status":200,"tier":"1","slack_channel":"#lab-identity","team":"identity","runbook":"https://runbooks.example.com/user-api","enriched_at":"2026-09-07T17:15:19.063Z"}
```

## Testing the DLQ paths

The generator already emits about 5% deliberately broken records, which exercise
two of the three DLQ routes (truncated JSON → `parse failure`, missing
`service`/`level` → `invalid`). To exercise the third, publish a record whose
service is not in the catalog:

```bash
printf '%s\n' '{"ts":"2026-09-07T17:20:00.000Z","service":"unknown-svc","host":"unknown-svc-1","level":"INFO","logger":"x.y","message":"request completed","trace_id":"aaaa","error_code":null,"latency_ms":5}' \
  | docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic logs.raw
```

It should appear in `logs.dlq` via LookupRecord's `unmatched` relationship.

## Troubleshooting

| Symptom | Cause |
|---------|-------|
| ConsumeKafka can't connect / times out | Bootstrap servers set to `localhost:9092` instead of `kafka:9093`. |
| No `parse failure` relationship on ConsumeKafka | It only appears once **Processing Strategy** is set to `RECORD`. Set that first, then wire the connection. |
| LookupRecord sends everything to `unmatched` | The `key` dynamic property is missing, or set to `service` instead of `/service`. |
| Enrichment fields nested under `root` | `Record Result Contents` is `Insert Entire Record`; change it to `Insert Record Fields`. |
| CSV service won't enable | Path typo. It must be `/opt/nifi/nifi-current/conf/service_catalog.csv`. |
| `Kafka3ConnectionService` won't enable / connection refused | `Security Protocol` left at its `SSL` default. Set it to `PLAINTEXT`. |
| A controller service doesn't appear in a processor's dropdown | It was created under Controller Settings (management scope). Recreate it in the process group via right-click canvas → Configure → Controller Services. |
| Malformed records fail at `PublishDlq` | `PublishDlq` has a Record Reader/Writer set; clear both. |
