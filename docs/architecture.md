# Architecture and design decisions

This document records *why* the platform is built the way it is. The README
shows what it does; this one explains the trade-offs, including the ones that
turned out to be wrong the first time.

---

## 1. Technology choices

| Layer | Choice | Alternative considered | Why this one |
| --- | --- | --- | --- |
| Field protocol | MQTT (Mosquitto 2.0) | HTTP, CoAP | Lightweight enough for a battery-powered microcontroller; QoS 1 and Last Will are exactly what an unreliable field link needs |
| Broker hosting | self-hosted Mosquitto | Adafruit IO, AWS IoT Core | Removes the dependency on a third-party cloud that the original thesis had — and whose API key leaked |
| Buffer | Kafka 3.9, KRaft mode | RabbitMQ, direct-to-storage | Retention and replay, independent consumer groups, ordering per key. KRaft because ZooKeeper is removed as of Kafka 4.0 |
| Kafka client (bridge) | confluent-kafka (librdkafka) | kafka-python | Only one implementing the idempotent producer; C binding, much faster |
| Stream processing | Spark Structured Streaming 3.5 | Flink, Kafka Streams | Batch and streaming share one API, so the same transformation functions are unit-testable outside a stream |
| Object storage | MinIO | local filesystem, HDFS | S3-compatible: the code is byte-identical against AWS S3, only the endpoint changes |
| File format | Parquet + Snappy | JSON, Avro | Columnar and compressed; partition pruning is the main performance lever on a lake |
| Serving store | MongoDB 7 | Redis, Postgres | Document-per-device fits the "latest state" query exactly; conditional updates make the write idempotent |
| Analytics engine | DuckDB | Trino, Spark SQL | Reads Parquet in S3 with no server to operate; adequate at this volume and trivial to run in CI |
| Transformations | dbt 1.9 | plain SQL scripts, Spark SQL | Dependency graph, schema tests, generated documentation and lineage |
| Orchestration | Airflow 3 | Prefect, Dagster, cron | Industry default, and Airflow 3 separates the DAG processor from the scheduler |
| ML | scikit-learn | Spark MLlib | The feature table is a few thousand rows; distributing the training would add cost and no benefit. The pipeline that *produces* the features is distributed — the training does not need to be |
| Metrics | Prometheus + Grafana | ELK, cloud-native | Pull model makes a dead service visible; dashboards can live in Git as JSON |

---

## 2. Delivery guarantees

### The acknowledgement order is the whole design

By default, a paho MQTT client acknowledges a message as soon as the callback is
entered. If the bridge dies between reception and the Kafka write, Mosquitto
considers the message delivered and never resends it — a silent loss.

The bridge therefore runs with `manual_ack=True` and calls `client.ack()` from
the Kafka **delivery callback**:

```
MQTT message received → written to Kafka → Kafka confirms → PUBACK sent
```

The consequence is accepted rather than fought: duplicates are expected.
QoS 1 means *at least once*, so deduplication downstream is a structural
necessity, not a precaution.

### Where idempotence lives

For the MongoDB serving layer, we do not use a stateful Spark operator. The
write itself is made idempotent:

```
1. $setOnInsert  creates the document if absent
2. $set          applies only if the stored ts is older
```

Replaying a batch is a no-op. This removed one state store — and therefore one
failure mode — from the streaming job. The general rule: use a stateful
operator only when the destination cannot absorb duplicates itself.

---

## 3. Time

### Event time, not processing time

`ts` is stamped by the sensor. A device that reconnects after three hours
offline emits readings with three-hour-old timestamps, and they must land in
the hour they belong to, not the hour they arrived.

### Watermark: 10 minutes

`dropDuplicates` must remember every key it has seen. On an infinite stream that
memory grows without bound. The watermark tells Spark it may forget state older
than 10 minutes.

The value is a trade-off: too short and legitimately late data is discarded, too
long and state grows. Ten minutes covers the brief network outages this
deployment sees. On a rural LoRa link, hours would be more appropriate.

**Watermark drops are silent by design.** A `StreamingQueryListener` exposes
`numRowsDroppedByWatermark` as a Prometheus counter so the loss is at least
observable.

### Quality rules must not read the clock

*This was wrong in the first implementation.* The plausibility gate compared
`ts` to `current_timestamp()`. The consequence: replaying the same data two days
later produced different verdicts, so the pipeline was not replayable.

It now compares `ts` to `kafka_ts`, the broker's ingestion time — a property
intrinsic to the record. The verdict is identical whenever it is computed.

> Caveat: `kafka_ts` is trustworthy here because the bridge does not set a
> message timestamp, so Kafka applies its own. A producer that set it explicitly
> would require switching the topic to `LogAppendTime`.

### Everything is UTC

`spark.sql.session.timeZone = UTC` is set explicitly. Without it Spark uses the
machine timezone and shifts every timestamp silently — a reading at 23:30 UTC
would land in the wrong `date=` partition. Local time is applied at the very
end, by Grafana.

---

## 4. Partitioning

### Kafka: keyed by `device_id`

All readings from one device land in the same partition and therefore stay
ordered. Parallelism is capped by the partition count (3).

> Partitioning depends on the client's hash function: CRC32 for librdkafka,
> murmur2 for the Java client. Two different producers writing the same key can
> target different partitions. Not an issue with a single producer, but a real
> trap when clients are mixed.

### Parquet: `site_id/date` for clean data, `quality_rule/date` for quarantine

Analytical queries almost always filter on a period and a site, so the engine
skips files outside the range. Quarantine is asked a different question — *which
kinds of defect, and when?* — hence a different partition key.

**Consequence: late-arriving data writes into a past partition.** A daily
aggregation cannot simply process "today's partition". This is the classic
late-arriving-data problem and it is why `daily_aggregation` rebuilds rather
than appends.

---

## 5. Storage and state

### What is configuration and what is data

| | Lives in | Survives `docker compose down -v` |
| --- | --- | --- |
| Topics, bucket, dashboards, datasource | files in this repository | yes, recreated at startup |
| Mosquitto password file | generated from `.env` by a script | yes, regenerable |
| Kafka log, Parquet, Mongo documents, metrics history | Docker named volumes | **no, deliberately destroyed** |

After a full reset, `ml/generate_history.py --seed 42` regenerates a byte-identical
synthetic history. The seed is fixed precisely for that reason.

### Checkpoints are a transactional unit

For a file sink, three things describe the same state and must be reset together:

- the Kafka offsets stored in `checkpointLocation`
- the `_spark_metadata` directory in the output path
- the state store of any stateful operator

Deleting one without the others produces `BATCH_METADATA_NOT_FOUND`, or — worse
— a query that reads nothing and reports no error because its stored offsets are
ahead of a recreated topic. Both happened during development. `scripts/reset-all.ps1`
exists so the reset is atomic.

Changing the query plan (adding `dropDuplicates`, changing a watermark) also
invalidates a checkpoint. In production this is handled by an explicit migration
or a deployment onto a new checkpoint.

### Checkpoints stay on a local volume, not S3

A streaming checkpoint performs many small writes and renames. S3 has no atomic
rename — it copies then deletes — which makes it slow and unsafe for this use.
On AWS the checkpoint would go to EFS or HDFS.

### Root credentials of stateful services are fixed at volume creation

`MINIO_ROOT_PASSWORD` is read only when the volume is initialised. Changing the
environment variable of an already-initialised container has no effect — the
same is true for Postgres, MySQL and MongoDB. Either recreate the volume or
change the password through the service's own tooling.

---

## 6. Security

- `.gitignore` was committed **before** the first `git add`. A secret committed
  once stays in history forever; the ordering is not a detail.
- Templates (`.env.example`, `secrets.h.example`) are versioned, real files are
  ignored. The template documents *which* variables are needed without
  revealing any value.
- Two Mosquitto accounts: `esp32` publishes, `bridge` reads. A device physically
  stolen from a field yields credentials that cannot drain the whole telemetry
  stream.
- Containers run as non-root where they build their own image. This has a cost:
  a named volume mounted on a directory that does not exist in the image is
  created as root, so `/checkpoints` is created and `chown`ed at build time,
  before `USER`.
- In production the `.env` file would be replaced by a secret manager (Vault,
  AWS Secrets Manager, Kubernetes Secrets). A file on disk is the reasonable
  local compromise.

---

## 7. Reproducibility

- Images are pinned, **including the base distribution**. `python:3.12-slim`
  silently moved from Debian bookworm to trixie during development and broke the
  build, because trixie dropped OpenJDK 17. The tag is now
  `python:3.12-slim-bookworm`.
- Init containers create Kafka topics and the MinIO bucket idempotently, so
  `docker compose up` on a fresh clone is sufficient.
- `depends_on: service_completed_successfully` makes startup deterministic:
  Spark cannot begin before the topics and the bucket exist.
- Ruff is pinned in CI. Linter defaults change between versions; a CI that is
  not deterministic is a CI nobody trusts.
- `.gitattributes` normalises line endings to LF. Without it, a shell script
  authored on Windows carries `CRLF` and fails inside a Linux container with
  `bash: \r: command not found`.

---

## 8. Testing strategy

Three tiers, deliberately separated:

| Tier | Needs | Where it runs |
| --- | --- | --- |
| Pure unit | nothing (stdlib, pandas) | locally and in CI, ~2 s |
| Spark | PySpark + Java 17 | streaming image locally, CI runner |
| Integration | the full Docker stack | manually, before a release |

Architecture is what makes testing possible. `ingestion/bridge/validation.py`
imports only the standard library, so testing the validation logic requires no
Kafka client. `ml/features.py` and `apply_quality_gates` are pure functions from
DataFrame to DataFrame. Whenever a test was hard to write, the code moved.

CI validates the infrastructure as well as the code: `docker compose config`
catches a broken YAML anchor or a variable missing from `.env.example` in ten
seconds — three development outages would have been caught by it.

---

## 9. Known limitations

- Single Kafka broker, replication factor 1. No fault tolerance; a real cluster
  would use 3 brokers and RF=3 with `min.insync.replicas=2`.
- Spark runs in `local[2]`. The job is written for a cluster but is not deployed
  on one.
- `ml/generate_history.py` writes directly to the lake, bypassing the streaming
  path. It is a development shortcut for producing enough training history, and
  it is labelled as such.
- Airflow and Grafana authentication are disabled for local convenience.
- Backfilled Parquet files are not registered in Spark's `_spark_metadata`, so a
  Spark reader using the metadata log would not see them. DuckDB and dbt, which
  read the files directly, do.
