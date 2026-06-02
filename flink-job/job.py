#!/usr/bin/env python3
"""
Real-Time ML Feature Engineering Pipeline — PyFlink Job (Fixed Version)
"""

import json
import logging
import os
from datetime import datetime, timezone

from pyflink.common import WatermarkStrategy, Types, Duration
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
    Time,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
    MapFunction,
)

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("FlinkFeatureJob")

# ---------------- Config ----------------
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
USER_EVENTS_TOPIC = os.environ.get("USER_EVENTS_TOPIC", "user-events")
METADATA_TOPIC = os.environ.get("CONTENT_METADATA_TOPIC", "content-metadata")
FEATURE_TOPIC = os.environ.get("FEATURE_STORE_TOPIC", "feature-store")

WATERMARK_SEC = int(os.environ.get("WATERMARK_SECONDS", "30"))

# ---------------- Helpers ----------------
def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_feature_record(entity_id, feature_name, feature_value):
    return json.dumps({
        "entity_id": entity_id,
        "feature_name": feature_name,
        "feature_value": feature_value,
        "computed_at": iso_now(),
    })

# ---------------- Timestamp Assigner ----------------
class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        try:
            data = json.loads(value)
            ts = data.get("timestamp")
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except:
            return record_timestamp

# ---------------- Aggregators ----------------
class UserFeatureAccumulator:
    def __init__(self):
        self.total = 0
        self.clicks = 0
        self.total_dwell = 0


class UserFeatureAggregator(AggregateFunction):
    def create_accumulator(self): return UserFeatureAccumulator()

    def add(self, value, acc):
        try:
            evt = json.loads(value)
            acc.total += 1
            acc.total_dwell += evt.get("dwell_time_ms", 0)
            if evt.get("event_type") == "click":
                acc.clicks += 1
        except:
            pass
        return acc

    def get_result(self, acc): return acc

    def merge(self, a, b):
        a.total += b.total
        a.clicks += b.clicks
        a.total_dwell += b.total_dwell
        return a


class UserFeatureWindowProcessor(ProcessWindowFunction):
    def process(self, key, context, elements):
        acc = next(iter(elements))
        click_rate = acc.clicks / acc.total if acc.total else 0.0
        avg_dwell = acc.total_dwell / acc.total if acc.total else 0.0

        yield make_feature_record(key, "click_rate", round(click_rate, 4))
        yield make_feature_record(key, "avg_dwell_time", round(avg_dwell, 2))

# ---------------- Kafka ----------------
def make_kafka_source(topic, group_id):
    return KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_SERVERS) \
        .set_topics(topic) \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


def make_kafka_sink(topic):
    return KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

# ---------------- MAIN ----------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)

    feature_sink = make_kafka_sink(FEATURE_TOPIC)

    # ✅ FIXED WATERMARK (IMPORTANT FIX)
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_SEC))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    stream = env.from_source(
        make_kafka_source(USER_EVENTS_TOPIC, "user-events-cg"),
        watermark_strategy,
        "KafkaSource"
    )

    # ---------------- USER FEATURES ----------------
    stream \
        .key_by(lambda x: json.loads(x).get("user_id", "unknown"), Types.STRING()) \
        .window(TumblingEventTimeWindows.of(Time.hours(1))) \
        .aggregate(
            UserFeatureAggregator(),
            UserFeatureWindowProcessor(),
            output_type=Types.STRING()
        ) \
        .sink_to(feature_sink)

    env.execute("Real-Time Feature Engineering Job")


if __name__ == "__main__":
    main()
