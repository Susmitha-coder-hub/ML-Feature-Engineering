#!/usr/bin/env python3
"""
Automated verification script — checks that all Kafka topics exist,
have correct cleanup policies, and messages conform to expected schemas.

Usage: python scripts/verify.py
Requires: confluent-kafka, jsonschema
"""

import json
import sys
import time
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

KAFKA_SERVERS = "localhost:9092"

EXPECTED_TOPICS = {
    "user-events":       {"partitions": 3, "cleanup.policy": None},         # standard
    "content-metadata":  {"partitions": 1, "cleanup.policy": "compact"},
    "feature-store":     {"partitions": 3, "cleanup.policy": "compact"},
}

USER_EVENT_SCHEMA = {"user_id", "content_id", "event_type", "dwell_time_ms", "timestamp"}
CONTENT_META_SCHEMA = {"content_id", "category", "creator_id", "publish_timestamp"}
FEATURE_STORE_SCHEMA = {"entity_id", "feature_name", "feature_value", "computed_at"}

OK  = "\033[92m✔\033[0m"
ERR = "\033[91m✗\033[0m"
WARN = "\033[93m!\033[0m"

errors = []


def check(condition, msg_ok, msg_fail):
    if condition:
        print(f"  {OK} {msg_ok}")
    else:
        print(f"  {ERR} {msg_fail}")
        errors.append(msg_fail)


def sample_topic(topic: str, n: int = 5) -> list:
    conf = {
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": f"verifier-{topic}-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    c.subscribe([topic])
    msgs = []
    polls = 0
    while len(msgs) < n and polls < 30:
        msg = c.poll(1.0)
        polls += 1
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            msgs.append(json.loads(msg.value().decode()))
        except Exception:
            pass
    c.close()
    return msgs


def main():
    print("=" * 55)
    print("  ML Pipeline Verification Script")
    print("=" * 55)

    admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})

    # 1. Topic existence and partition count
    print("\n[1] Topic Existence")
    metadata = admin.list_topics(timeout=10)
    for topic, config in EXPECTED_TOPICS.items():
        exists = topic in metadata.topics
        check(exists, f"{topic} exists", f"{topic} MISSING")
        if exists:
            parts = len(metadata.topics[topic].partitions)
            check(
                parts >= config["partitions"],
                f"{topic} has {parts} partitions (≥{config['partitions']})",
                f"{topic} has only {parts} partitions, expected ≥{config['partitions']}",
            )

    # 2. Cleanup policies
    print("\n[2] Cleanup Policies")
    from confluent_kafka.admin import ConfigResource, ConfigSource
    resources = [
        ConfigResource("topic", t)
        for t, c in EXPECTED_TOPICS.items()
        if c["cleanup.policy"]
    ]
    if resources:
        futures = admin.describe_configs(resources)
        for res, future in futures.items():
            try:
                cfg = future.result()
                policy = cfg.get("cleanup.policy", None)
                val = policy.value if policy else None
                expected = EXPECTED_TOPICS[res.name]["cleanup.policy"]
                check(
                    val == expected,
                    f"{res.name} cleanup.policy={val}",
                    f"{res.name} cleanup.policy={val}, expected {expected}",
                )
            except Exception as e:
                print(f"  {WARN} Could not read config for {res.name}: {e}")

    # 3. Schema validation
    print("\n[3] Message Schema Validation")

    def validate_schema(topic, expected_keys, n=5):
        samples = sample_topic(topic, n)
        if not samples:
            print(f"  {WARN} {topic}: no messages yet (pipeline may still be starting)")
            return
        ok = all(expected_keys.issubset(set(m.keys())) for m in samples)
        check(ok, f"{topic} schema valid ({len(samples)} samples)", f"{topic} schema invalid")
        if not ok:
            for m in samples:
                missing = expected_keys - set(m.keys())
                if missing:
                    print(f"      Missing keys: {missing}")

    validate_schema("user-events",      USER_EVENT_SCHEMA)
    validate_schema("content-metadata", CONTENT_META_SCHEMA)
    validate_schema("feature-store",    FEATURE_STORE_SCHEMA)

    # 4. Late event check
    print("\n[4] Late Event Detection")
    raw = sample_topic("user-events", 100)
    if raw:
        from datetime import datetime, timezone, timedelta
        tss = []
        for m in raw:
            try:
                tss.append(datetime.strptime(m["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc))
            except Exception:
                pass
        if tss:
            max_ts = max(tss)
            late = [t for t in tss if (max_ts - t).total_seconds() >= 35]
            ratio = len(late) / len(tss)
            check(
                ratio >= 0.05,
                f"Late events: {len(late)}/{len(tss)} = {ratio*100:.1f}% (≥5% required)",
                f"Late events only {ratio*100:.1f}%, expected ≥5%",
            )

    # 5. Feature store features
    print("\n[5] Feature Store Content")
    features = sample_topic("feature-store", 50)
    if features:
        names = {f["feature_name"] for f in features}
        for expected_feat in ["click_rate", "avg_dwell_time", "engagement_rate"]:
            check(
                expected_feat in names,
                f"Feature '{expected_feat}' present in feature-store",
                f"Feature '{expected_feat}' NOT found in feature-store",
            )

    # Summary
    print("\n" + "=" * 55)
    if errors:
        print(f"  {ERR} {len(errors)} check(s) FAILED:")
        for e in errors:
            print(f"    - {e}")
        sys.exit(1)
    else:
        print(f"  {OK} All checks passed!")


if __name__ == "__main__":
    main()
