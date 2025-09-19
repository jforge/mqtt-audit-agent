# Flink SQL zero-loss audit (docker-compose)

## 1) Start stack
```bash
docker compose up -d
```

## 2) Create Kafka topics
```bash
docker compose exec kafka kafka-topics.sh --create --topic events --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
/docker-entrypoint.sh kafka-topics.sh --create --topic audit_counts --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
/docker-entrypoint.sh kafka-topics.sh --create --topic audit_status --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
/docker-entrypoint.sh kafka-topics.sh --create --topic audit_mismatches --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

## 3) Run the SQL job
```bash
docker compose exec sql-client ./bin/sql-client.sh -l rest -e /sql/job.sql
```
This submits the streaming statements to the JobManager (Flink UI on http://localhost:8081).

## 4) Feed data
- Ensure your analytical platform writes incoming events to Kafka topic `events` with JSON containing `ts_event`.
- Bridge the audit agent summaries to Kafka topic `audit_counts` (use your existing ingestion or a small MQTT→Kafka bridge like Kafka Connect MQTT Source).

## 5) Observe & alert
- Kafka UI: http://localhost:8085 (check topics `audit_status` & `audit_mismatches`).
- Prometheus: http://localhost:9090 (query the alert expression).
- Alertmanager: http://localhost:9093 (wire a real receiver instead of the placeholder webhook).
- Flink UI: http://localhost:8081.

## 6) Tuning
- Window size (must match audit agent): change `INTERVAL '5' SECOND` in `/sql/job.sql`.
- Lateness tolerance: adjust watermark in table `events`.
- Throughput: increase `parallelism.default` and Kafka partitions.

## Notes
- This stack keeps brokers untouched. The heavy lifting (windowing, joins, alerts) happens in Flink/Prometheus.
- If you prefer Redpanda (no ZooKeeper), we can swap the Kafka/ZK pair for a single Redpanda container and adapt exporters accordingly.
