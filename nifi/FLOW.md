# NiFi flow build sheet

The flow isn't in version control — it lives in a Docker volume. Rebuild it from
this sheet. Values verified against `apache/nifi:2.11.0`. Anything not listed
stays at its default.

## Controller services

Right-click empty canvas → **Configure** → **Controller Services** → **+**.
Create all four, then enable each with the lightning-bolt icon.

| Type | Name | Property | Value |
|---|---|---|---|
| `JsonTreeReader` | `JsonReader` | *(defaults)* | |
| `JsonRecordSetWriter` | `JsonWriter` | *(defaults)* | |
| `Kafka3ConnectionService` | `KafkaConn` | Bootstrap Servers | `kafka:9093` |
| | | Security Protocol | `PLAINTEXT` (default is `SSL`) |
| `CSVRecordLookupService` | `SvcCatalog` | CSV File | `/opt/nifi/nifi-current/conf/service_catalog.csv` |
| | | Lookup Key Column | `service` |
| | | CSV Format | `default` |

Create them here, not under Controller Settings — those are flow-controller
scoped and won't show up in processor dropdowns.

## Processors

### ConsumeKafka

| Property | Value |
|---|---|
| Kafka Connection Service | `KafkaConn` |
| Group ID | `nifi-log-lab` |
| Topic Format | `names` |
| Topics | `logs.raw` |
| Processing Strategy | `RECORD` |
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |
| Auto Offset Reset | `earliest` |

### ValidateRecord

| Property | Value |
|---|---|
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |
| Schema Access Strategy | `Use "Schema Text" Property` |
| Schema Text | the schema below |
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

### LookupRecord

| Property | Value |
|---|---|
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |
| Lookup Service | `SvcCatalog` |
| Routing Strategy | `Route to 'matched' or 'unmatched'` |
| Record Result Contents | `Insert Record Fields` |
| Record Update Strategy | `Use Property` |
| Result RecordPath | `/` |

Plus a dynamic property (the **+** in the properties tab) — easy to miss:

| Name | Value |
|---|---|
| `key` | `/service` |

### UpdateRecord

| Property | Value |
|---|---|
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |
| Replacement Value Strategy | `Literal Value` |

Plus a dynamic property:

| Name | Value |
|---|---|
| `/enriched_at` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "GMT")}` |

### QueryRecord

| Property | Value |
|---|---|
| Record Reader | `JsonReader` |
| Record Writer | `JsonWriter` |

Plus two dynamic properties — each creates a relationship of the same name:

| Name | Value |
|---|---|
| `alerts` | `SELECT * FROM FLOWFILE WHERE level IN ('WARN','ERROR')` |
| `enriched` | `SELECT * FROM FLOWFILE WHERE level NOT IN ('WARN','ERROR')` |

Auto-terminate `original` and `failure`.

### PublishKafka × 4

| Name | Topic Name | Record Reader / Writer |
|---|---|---|
| `PublishEnriched` | `logs.enriched` | `JsonReader` / `JsonWriter` |
| `PublishAlerts` | `logs.alerts` | `JsonReader` / `JsonWriter` |
| `PublishDlqInvalid` | `logs.dlq.invalid` | *leave both empty* |
| `PublishDlqUnmatched` | `logs.dlq.unmatched` | *leave both empty* |

All four also get Kafka Connection Service `KafkaConn` and Transactions Enabled
`false`. Auto-terminate `success` and `failure` on each. The DLQ publishers have
no reader or writer on purpose — they must pass through content that isn't valid
JSON.

## Connections

| From | Relationship(s) | To |
|---|---|---|
| ConsumeKafka | `success` | ValidateRecord |
| ConsumeKafka | `parse failure` | PublishDlqInvalid |
| ValidateRecord | `valid` | LookupRecord |
| ValidateRecord | `invalid`, `failure` | PublishDlqInvalid |
| LookupRecord | `matched` | UpdateRecord |
| LookupRecord | `unmatched` | PublishDlqUnmatched |
| LookupRecord | `failure` | PublishDlqInvalid |
| UpdateRecord | `success` | QueryRecord |
| UpdateRecord | `failure` | PublishDlqInvalid |
| QueryRecord | `alerts` | PublishAlerts |
| QueryRecord | `enriched` | PublishEnriched |

A processor stays invalid until every relationship is connected or
auto-terminated. That's normal while wiring up.

## Check it works

Start everything, then from the repo root:

```bash
uv run generator/produce_logs.py --count 60 --rate 20

for t in logs.raw logs.enriched logs.alerts logs.dlq.invalid logs.dlq.unmatched; do
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$t"
done
```

Output counts should add up to what went into `logs.raw`. A 60-event run gave
49 enriched, 8 alerts, 3 invalid.

To exercise `logs.dlq.unmatched`, send a service that isn't in the catalog:

```bash
printf '%s\n' '{"ts":"2026-09-07T17:20:00.000Z","service":"unknown-svc","host":"unknown-svc-1","level":"INFO","logger":"x.y","message":"request completed","trace_id":"aaaa","error_code":null,"latency_ms":5}' \
  | docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic logs.raw
```

## Troubleshooting

| Symptom | Cause |
|---|---|
| ConsumeKafka times out | Bootstrap servers set to `localhost:9092` instead of `kafka:9093`. |
| No `parse failure` relationship | Set **Processing Strategy** to `RECORD` first, then wire it. |
| Everything goes to `unmatched` | The `key` dynamic property is missing or set to `service` instead of `/service`. |
| Enrichment fields nested under `root` | `Record Result Contents` is `Insert Entire Record`; use `Insert Record Fields`. |
| CSV service won't enable | Path typo. Must be `/opt/nifi/nifi-current/conf/service_catalog.csv`. |
| `Kafka3ConnectionService` won't enable | `Security Protocol` left at `SSL`. Set `PLAINTEXT`. |
| A controller service is missing from a dropdown | It was created under Controller Settings. Recreate it via right-click canvas → Configure → Controller Services. |
| Broken records fail at a DLQ publisher | It has a Record Reader/Writer set; clear both. |
| Publish fails with unknown topic | Auto-creation is off; run `docker compose up kafka-init`. |
