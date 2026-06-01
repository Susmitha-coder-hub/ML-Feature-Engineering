#!/usr/bin/env python3
"""
Real-Time ML Feature Engineering Pipeline — PyFlink Job

Computes:
  1. Per-user features via 1-hour tumbling windows  (click_rate, avg_dwell_time)
  2. Per-content features via 15-min/5-min sliding windows (engagement_rate)
  3. Category affinity via stream-table join + 1-hour tumbling window

Watermark strategy: BoundedOutOfOrderness(30 seconds)
All computed features are written to the feature-store Kafka topic.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta

from pyflink.common import WatermarkStrategy, Time, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    SlidingEventTimeWindows,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
    MapFunction,
    FilterFunction,
)
from pyflink.table import StreamTableEnvironment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("FlinkFeatureJob")

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS      = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
USER_EVENTS_TOPIC  = os.environ.get("USER_EVENTS_TOPIC", "user-events")
METADATA_TOPIC     = os.environ.get("CONTENT_METADATA_TOPIC", "content-metadata")
FEATURE_TOPIC      = os.environ.get("FEATURE_STORE_TOPIC", "feature-store")
METRICS_TOPIC      = os.environ.get("FLINK_METRICS_TOPIC", "flink-metrics")
WATERMARK_SEC      = int(os.environ.get("WATERMARK_SECONDS", "30"))

# ── Helpers ───────────────────────────────────────────────────────────────────

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_feature_record(entity_id: str, feature_name: str, feature_value) -> str:
    return json.dumps({
        "entity_id": entity_id,
        "feature_name": feature_name,
        "feature_value": feature_value,
        "computed_at": iso_now(),
    })


def feature_key(entity_id: str, feature_name: str) -> str:
    return f"{entity_id}:{feature_name}"


# ── Timestamp Assigner ────────────────────────────────────────────────────────

class EventTimestampAssigner(TimestampAssigner):
    """Extract ISO-8601 timestamp from the 'timestamp' field of the JSON event."""

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        try:
            ts_str = json.loads(value).get("timestamp", "")
            # Parse ISO 8601 — format: 2024-01-01T12:00:00Z
            dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp


# ── User Feature Aggregators (Tumbling 1-hour) ───────────────────────────────

class UserFeatureAccumulator:
    __slots__ = ["total", "clicks", "total_dwell", "late_count"]

    def __init__(self):
        self.total      = 0
        self.clicks     = 0
        self.total_dwell = 0
        self.late_count  = 0


class UserFeatureAggregator(AggregateFunction):

    def create_accumulator(self):
        return UserFeatureAccumulator()

    def add(self, value, acc: UserFeatureAccumulator):
        try:
            evt = json.loads(value)
            acc.total       += 1
            acc.total_dwell += evt.get("dwell_time_ms", 0)
            if evt.get("event_type") == "click":
                acc.clicks += 1
        except Exception:
            pass
        return acc

    def get_result(self, acc: UserFeatureAccumulator):
        return acc

    def merge(self, a: UserFeatureAccumulator, b: UserFeatureAccumulator):
        a.total       += b.total
        a.clicks      += b.clicks
        a.total_dwell += b.total_dwell
        return a


class UserFeatureWindowProcessor(ProcessWindowFunction):
    """Emit click_rate and avg_dwell_time per user per window."""

    def process(self, key, context, elements):
        acc: UserFeatureAccumulator = next(iter(elements))
        click_rate    = acc.clicks / acc.total if acc.total > 0 else 0.0
        avg_dwell     = acc.total_dwell / acc.total if acc.total > 0 else 0.0
        window_start  = context.window().start
        window_end    = context.window().end

        log.info(
            "[UserFeature] user=%s window=[%s,%s] total=%d click_rate=%.3f avg_dwell=%.0f",
            key, window_start, window_end, acc.total, click_rate, avg_dwell,
        )

        yield make_feature_record(key, "click_rate", round(click_rate, 4))
        yield make_feature_record(key, "avg_dwell_time", round(avg_dwell, 2))
        yield make_feature_record(key, "event_count", acc.total)


# ── Content Feature Aggregators (Sliding 15-min / 5-min) ─────────────────────

class ContentFeatureAccumulator:
    __slots__ = ["views", "likes", "shares"]

    def __init__(self):
        self.views  = 0
        self.likes  = 0
        self.shares = 0


class ContentFeatureAggregator(AggregateFunction):

    def create_accumulator(self):
        return ContentFeatureAccumulator()

    def add(self, value, acc: ContentFeatureAccumulator):
        try:
            evt = json.loads(value)
            etype = evt.get("event_type", "")
            if etype == "view":
                acc.views  += 1
            elif etype == "like":
                acc.likes  += 1
            elif etype == "share":
                acc.shares += 1
        except Exception:
            pass
        return acc

    def get_result(self, acc: ContentFeatureAccumulator):
        return acc

    def merge(self, a: ContentFeatureAccumulator, b: ContentFeatureAccumulator):
        a.views  += b.views
        a.likes  += b.likes
        a.shares += b.shares
        return a


class ContentFeatureWindowProcessor(ProcessWindowFunction):
    """Emit engagement_rate per content item per sliding window."""

    def process(self, key, context, elements):
        acc: ContentFeatureAccumulator = next(iter(elements))
        engagement = (
            (acc.likes + acc.shares) / acc.views if acc.views > 0 else 0.0
        )
        yield make_feature_record(key, "engagement_rate", round(engagement, 4))
        yield make_feature_record(key, "view_count",      acc.views)
        yield make_feature_record(key, "like_count",      acc.likes)
        yield make_feature_record(key, "share_count",     acc.shares)


# ── Category Affinity via Stream-Table Join ───────────────────────────────────

class CategoryAffinityAccumulator:
    def __init__(self):
        self.counts: dict = {}  # category → count

    def add(self, category: str):
        self.counts[category] = self.counts.get(category, 0) + 1


class CategoryAffinityAggregator(AggregateFunction):

    def create_accumulator(self):
        return CategoryAffinityAccumulator()

    def add(self, value, acc: CategoryAffinityAccumulator):
        # value is a JSON string of the enriched event: {user_id, category, ...}
        try:
            rec = json.loads(value)
            cat = rec.get("category", "unknown")
            acc.add(cat)
        except Exception:
            pass
        return acc

    def get_result(self, acc: CategoryAffinityAccumulator):
        return acc

    def merge(self, a: CategoryAffinityAccumulator, b: CategoryAffinityAccumulator):
        for cat, cnt in b.counts.items():
            a.counts[cat] = a.counts.get(cat, 0) + cnt
        return a


class CategoryAffinityProcessor(ProcessWindowFunction):
    """Emit one feature record per user per category."""

    def process(self, key, context, elements):
        acc: CategoryAffinityAccumulator = next(iter(elements))
        for category, count in acc.counts.items():
            feature_name = f"category_affinity_{category}"
            yield make_feature_record(key, feature_name, count)


# ── Flink Kafka Factory Helpers ───────────────────────────────────────────────

def make_kafka_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def make_kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Flink Feature Engineering Job starting ===")
    log.info("Kafka=%s | WatermarkTolerance=%ds", KAFKA_SERVERS, WATERMARK_SEC)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)
    env.enable_checkpointing(30_000)  # 30s checkpoint interval

    t_env = StreamTableEnvironment.create(env)

    # ── Feature Store Sink ──
    feature_sink = make_kafka_sink(FEATURE_TOPIC)

    # ── Watermark strategy: BoundedOutOfOrderness(WATERMARK_SEC seconds) ──
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Time.seconds(WATERMARK_SEC))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    # ── User Events Source ──
    user_events_source = make_kafka_source(USER_EVENTS_TOPIC, "flink-user-events-cg")
    user_events_stream = (
        env
        .from_source(user_events_source, watermark_strategy, "UserEventsSource")
        .name("UserEventsSource")
    )

    # ═══════════════════════════════════════════════════════════════════
    # FEATURE 1: Per-user click_rate & avg_dwell_time (Tumbling 1-hour)
    # ═══════════════════════════════════════════════════════════════════
    user_features = (
        user_events_stream
        .key_by(lambda raw: json.loads(raw).get("user_id", "unknown"),
                key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .aggregate(
            UserFeatureAggregator(),
            UserFeatureWindowProcessor(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.STRING(),
        )
        .name("UserFeatureTumblingWindow")
    )
    user_features.sink_to(feature_sink).name("UserFeatureSink")

    # ═══════════════════════════════════════════════════════════════════
    # FEATURE 2: Per-content engagement_rate (Sliding 15min / 5min)
    # ═══════════════════════════════════════════════════════════════════
    content_features = (
        user_events_stream
        .key_by(lambda raw: json.loads(raw).get("content_id", "unknown"),
                key_type=Types.STRING())
        .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
        .aggregate(
            ContentFeatureAggregator(),
            ContentFeatureWindowProcessor(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.STRING(),
        )
        .name("ContentFeatureSlidingWindow")
    )
    content_features.sink_to(feature_sink).name("ContentFeatureSink")

    # ═══════════════════════════════════════════════════════════════════
    # FEATURE 3: Category affinity via Stream-Table join
    # ═══════════════════════════════════════════════════════════════════

    # Register content metadata topic as a Flink SQL table (changelog stream)
    t_env.execute_sql(f"""
        CREATE TABLE content_metadata (
            content_id        STRING,
            category          STRING,
            creator_id        STRING,
            publish_timestamp STRING,
            PRIMARY KEY (content_id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{METADATA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_SERVERS}',
            'properties.group.id' = 'flink-metadata-cg',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'json',
            'value.json.ignore-parse-errors' = 'true'
        )
    """)

    # Register user events as a SQL table for the join
    t_env.execute_sql(f"""
        CREATE TABLE user_events_table (
            user_id       STRING,
            content_id    STRING,
            event_type    STRING,
            dwell_time_ms BIGINT,
            `timestamp`   STRING,
            event_time    AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
            WATERMARK FOR event_time AS event_time - INTERVAL '{WATERMARK_SEC}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{USER_EVENTS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_SERVERS}',
            'properties.group.id' = 'flink-events-join-cg',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'json',
            'value.json.ignore-parse-errors' = 'true'
        )
    """)

    # Enrich events with category via temporal join on content metadata
    enriched_table = t_env.sql_query("""
        SELECT
            u.user_id,
            u.content_id,
            u.event_type,
            u.dwell_time_ms,
            COALESCE(m.category, 'unknown') AS category,
            u.event_time
        FROM user_events_table u
        LEFT JOIN content_metadata FOR SYSTEM_TIME AS OF u.event_time m
            ON u.content_id = m.content_id
    """)

    # Convert enriched table back to DataStream for the windowed aggregation
    enriched_stream = t_env.to_data_stream(enriched_table)

    # Serialize enriched rows to JSON for reuse with existing aggregators
    class EnrichedRowToJson(MapFunction):
        def map(self, row):
            return json.dumps({
                "user_id":    row[0],
                "content_id": row[1],
                "event_type": row[2],
                "category":   row[4],
            })

    enriched_json = (
        enriched_stream
        .map(EnrichedRowToJson(), output_type=Types.STRING())
        .name("EnrichedRowsToJson")
    )

    # Re-watermark enriched stream (to_data_stream loses watermarks)
    class EnrichedTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp: int) -> int:
            return record_timestamp  # use Flink's internal time after SQL

    rewatermarked = enriched_json.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Time.seconds(WATERMARK_SEC))
        .with_timestamp_assigner(EnrichedTimestampAssigner())
    ).name("EnrichedRewatermark")

    category_affinity = (
        rewatermarked
        .key_by(lambda raw: json.loads(raw).get("user_id", "unknown"),
                key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .aggregate(
            CategoryAffinityAggregator(),
            CategoryAffinityProcessor(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.STRING(),
        )
        .name("CategoryAffinityTumblingWindow")
    )
    category_affinity.sink_to(feature_sink).name("CategoryAffinitySink")

    log.info("Executing Flink job graph…")
    env.execute("RealTimeFeatureEngineeringJob")


if __name__ == "__main__":
    main()
