# MQTT Audit Agent (K8s)

A minimal Go service that subscribes to MQTT topics, counts messages in **event-time tumbling windows**, and publishes per-window **audit summaries** to `_audit/counts/...`. Designed to keep load **off the broker** and push verification work to your analytics (e.g., Flink SQL) to prove **zero loss** by comparing counts.

## Build & Push
```bash
  # build
docker build -t ghcr.io/your-org/mqtt-audit-agent:latest .
# push
docker push ghcr.io/your-org/mqtt-audit-agent:latest
```

## Deploy
```bash
  kubectl apply -f k8s/deployment.yaml
```

## Config
Key env vars:
- `BROKER_URL` e.g. `tcp://broker:1883` or `ssl://broker:8883`
- `TOPIC_FILTERS` comma-separated MQTT filters (e.g., `factory/+/sensor/#`)
- `AUDIT_TOPIC_PREFIX` default `_audit/counts`
- `WINDOW_SECONDS` tumbling window size (default 5)
- `LATENESS_SECONDS` grace before closing/publishing (default 20)
- `EVENT_TIME_JSON_FIELD` JSON key with event-time (default `timestamp`)
- `EVENT_TIME_FORMAT` one of `rfc3339`, `unix_ms`, `unix_s`

## Output
Publishes JSON like:
```json
{
  "topic_pattern": "factory/+/sensor/#",
  "window_start": "2025-09-16T12:34:00Z",
  "window_end":   "2025-09-16T12:34:05Z",
  "count": 12345,
  "agent_id": "audit-agent-1",
  "watermark_seen": "2025-09-16T12:34:08Z",
  "payload_crc": "<optional sha1 base64>"
}
```

Published on topic `_audit/counts/factory_+plus_sensor_hash` (filter encoded for safety).

## Notes
- Uses persistent session (`CleanSession=false`) and QoS 1 for summaries.
- Counts are **event-time** (from payload) with `LATENESS_SECONDS` grace; if event-time missing, falls back to receive time.
- Keep filters as **non-overlapping** to avoid double counting; otherwise the agent counts a message in every matching filter.
- Health endpoint at `/healthz` (port 8080).