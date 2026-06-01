# Real-Time ML Feature Engineering Pipeline

A production-style streaming pipeline that computes ML features in real time using **Apache Kafka** and **Apache Flink** (PyFlink). All components run in Docker and can be started with a single command.

## Architecture

```
Producer ──► user-events (Kafka) ──► Flink Job ──► feature-store (Kafka) ──► Dashboard
              content-metadata ────►  (enrichment)
```

| Component | Technology | Role |
|---|---|---|
| Producer | Python + confluent-kafka | Simulates user events with late-event injection |
| Kafka | Confluent Kafka 7.4 | Event bus & lightweight feature store |
| Flink | PyFlink 1.17 | Stateful stream processing (windows, join) |
| Dashboard | FastAPI + WebSockets | Real-time observability UI |

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env          # edit if needed

# 2. Launch everything
docker-compose up --build -d

# 3. Watch services come healthy (< 5 min)
docker-compose ps

# 4. Open dashboard
open http://localhost:8080

# 5. Flink UI (optional)
open http://localhost:8081
```

## Kafka Topics

| Topic | Partitions | Cleanup Policy | Purpose |
|---|---|---|---|
| `user-events` | 3 | delete | Raw interaction stream |
| `content-metadata` | 1 | **compact** | Content lookup table |
| `feature-store` | 3 | **compact** | Computed ML features |
| `flink-metrics` | 1 | delete | Watermark & late-event metrics |

## Features Computed

### Per-User (1-hour tumbling window)
| Feature | Description |
|---|---|
| `click_rate` | clicks / total_events |
| `avg_dwell_time` | average dwell_time_ms |
| `event_count` | total interactions |
| `category_affinity_<cat>` | count of events per content category |

### Per-Content (15-min sliding, 5-min step)
| Feature | Description |
|---|---|
| `engagement_rate` | (likes + shares) / views |
| `view_count` | total views in window |
| `like_count` | total likes in window |
| `share_count` | total shares in window |

## Message Schemas

### user-events
```json
{
  "user_id": "user_001",
  "content_id": "content_042",
  "event_type": "view",
  "dwell_time_ms": 45000,
  "timestamp": "2024-06-01T13:00:00Z"
}
```

### content-metadata
```json
{
  "content_id": "content_001",
  "category": "sci-fi",
  "creator_id": "creator_003",
  "publish_timestamp": "2024-03-15T08:00:00Z"
}
```

### feature-store
```json
{
  "entity_id": "user_001",
  "feature_name": "click_rate",
  "feature_value": 0.2143,
  "computed_at": "2024-06-01T14:00:05Z"
}
```

## Dashboard

Open **http://localhost:8080** to see:

- **Pipeline Health** — Flink job status, total features computed, late events dropped, watermark lag
- **Feature Freshness** — seconds since last update for key features
- **Entity Viewer** — enter `user_001` or `content_001` to see all features
- **Live Feature Stream** — real-time feed of feature updates

## Configuration

All settings are in `.env` (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `TIME_ACCELERATION` | 60 | Sim-minutes per real second |
| `LATE_EVENT_RATIO` | 0.07 | Fraction of events that are late |
| `LATE_EVENT_DELAY_MIN_SECONDS` | 35 | Min delay for late events |
| `LATE_EVENT_DELAY_MAX_SECONDS` | 90 | Max delay for late events |
| `WATERMARK_SECONDS` | 30 | Flink watermark tolerance |
| `NUM_USERS` | 20 | Simulated users |
| `NUM_CONTENT` | 50 | Content items |

## Verification

```bash
# Install deps locally
pip install confluent-kafka jsonschema

# Run automated checks
python scripts/verify.py

# Run batch comparison
cd batch-analysis && pip install confluent-kafka pandas
python batch_analysis.py
```

## Test IDs (submission.json)

```json
{ "test_user_id": "user_001", "test_content_id": "content_001" }
```

Both IDs are seeded deterministically, so the producer always generates events for them.

## Stopping

```bash
docker-compose down -v   # remove volumes too
```

## Analysis

See [ANALYSIS.md](ANALYSIS.md) for a detailed comparison of batch vs. streaming feature values and an explanation of the watermark and late-event handling strategy.
