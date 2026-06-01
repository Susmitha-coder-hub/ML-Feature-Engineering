#!/usr/bin/env python3
"""
Batch Feature Computation Script
Reads events from the Kafka topic, computes features using pandas,
and compares them with values from the feature-store topic.

Usage:
    pip install confluent-kafka pandas
    python batch_analysis.py
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import pandas as pd
from confluent_kafka import Consumer, TopicPartition, KafkaError

KAFKA_SERVERS  = "localhost:9092"
USER_EVENTS    = "user-events"
FEATURE_STORE  = "feature-store"
WINDOW_HOURS   = 1
SLIDE_MINUTES  = 15
SLIDE_STEP     = 5
MAX_MESSAGES   = 50_000

# ── Kafka helpers ────────────────────────────────────────────────────────────

def drain_topic(topic: str, group_id: str, max_msgs: int) -> list:
    conf = {
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    records = []
    empty_polls = 0
    while len(records) < max_msgs and empty_polls < 5:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
            continue
        empty_polls = 0
        try:
            records.append(json.loads(msg.value().decode()))
        except Exception:
            pass
    consumer.close()
    print(f"  Drained {len(records)} records from '{topic}'")
    return records


# ── Batch feature computation ─────────────────────────────────────────────────

def compute_user_features_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Compute click_rate and avg_dwell_time in 1-hour tumbling windows."""
    df = df.copy()
    df["window"] = df["event_time"].dt.floor("1H")
    df["is_click"] = (df["event_type"] == "click").astype(int)

    agg = df.groupby(["user_id", "window"]).agg(
        total_events=("event_type", "count"),
        clicks=("is_click", "sum"),
        total_dwell=("dwell_time_ms", "sum"),
    ).reset_index()

    agg["click_rate"]     = agg["clicks"] / agg["total_events"]
    agg["avg_dwell_time"] = agg["total_dwell"] / agg["total_events"]
    return agg


def compute_content_features_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Compute engagement_rate in 15-min sliding windows, step 5 min."""
    df = df.copy()
    results = []
    min_ts = df["event_time"].min()
    max_ts = df["event_time"].max()

    window_start = min_ts.floor("5min")
    while window_start <= max_ts:
        window_end = window_start + timedelta(minutes=15)
        mask = (df["event_time"] >= window_start) & (df["event_time"] < window_end)
        w_df = df[mask]
        if not w_df.empty:
            for cid, grp in w_df.groupby("content_id"):
                views  = (grp["event_type"] == "view").sum()
                likes  = (grp["event_type"] == "like").sum()
                shares = (grp["event_type"] == "share").sum()
                engagement = (likes + shares) / views if views > 0 else 0.0
                results.append({
                    "content_id": cid,
                    "window_start": window_start,
                    "window_end": window_end,
                    "engagement_rate": engagement,
                    "views": views,
                })
        window_start += timedelta(minutes=5)

    return pd.DataFrame(results)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Batch Feature Analysis")
    print("=" * 60)

    # Drain event data
    print("\n1. Reading events from Kafka...")
    raw_events = drain_topic(USER_EVENTS, "batch-analysis-cg", MAX_MESSAGES)
    if not raw_events:
        print("  No events found. Is the producer running?")
        sys.exit(1)

    df = pd.DataFrame(raw_events)
    df["event_time"] = pd.to_datetime(df["timestamp"], utc=True)
    print(f"  Time range: {df['event_time'].min()} → {df['event_time'].max()}")
    print(f"  Unique users:   {df['user_id'].nunique()}")
    print(f"  Unique content: {df['content_id'].nunique()}")
    print(f"  Event types:\n{df['event_type'].value_counts().to_string()}")

    # Detect late events
    late = df[df["event_time"] < (df["event_time"].max() - timedelta(seconds=35))]
    print(f"\n  Late events (>35s behind max): {len(late)} ({100*len(late)/len(df):.1f}%)")

    # Batch computations
    print("\n2. Computing batch features...")
    user_feats    = compute_user_features_batch(df)
    content_feats = compute_content_features_batch(df)

    print("\n── User Features (sample) ─────────────────────────────────")
    print(user_feats[["user_id","window","click_rate","avg_dwell_time"]].head(10).to_string(index=False))

    print("\n── Content Features (sample) ─────────────────────────────")
    print(content_feats[["content_id","window_start","engagement_rate","views"]].head(10).to_string(index=False))

    # Compare with streaming feature-store
    print("\n3. Reading streaming features from feature-store...")
    stream_records = drain_topic(FEATURE_STORE, "batch-compare-cg", MAX_MESSAGES)
    if not stream_records:
        print("  No streaming features found. Is the Flink job running?")
        return

    stream_df = pd.DataFrame(stream_records)
    user_stream = stream_df[stream_df["feature_name"].isin(["click_rate","avg_dwell_time"])]

    print("\n── Streaming User Features (sample) ──────────────────────")
    print(user_stream[["entity_id","feature_name","feature_value","computed_at"]].head(10).to_string(index=False))

    # Divergence analysis
    print("\n4. Divergence Analysis")
    print("-" * 40)

    if not user_stream.empty:
        cr_stream = user_stream[user_stream["feature_name"]=="click_rate"]
        cr_batch  = user_feats[["user_id","click_rate"]]

        merged = cr_stream.merge(cr_batch, left_on="entity_id", right_on="user_id", suffixes=("_stream","_batch"))
        if not merged.empty:
            merged["delta"] = (merged["feature_value"] - merged["click_rate"]).abs()
            print("click_rate divergence (stream vs batch):")
            print(merged[["entity_id","feature_value","click_rate","delta"]].head(10).to_string(index=False))
            print(f"\nMean absolute delta: {merged['delta'].mean():.4f}")
        else:
            print("Not enough overlap for comparison yet.")

    print("\n5. Key Observations Written to batch_results.txt")
    with open("batch_results.txt", "w") as f:
        f.write("=== Batch Analysis Results ===\n\n")
        f.write(f"Total events processed: {len(df)}\n")
        f.write(f"Late events detected:   {len(late)} ({100*len(late)/len(df):.1f}%)\n\n")
        f.write("User Features (all):\n")
        f.write(user_feats.to_string(index=False))
        f.write("\n\nContent Features (all):\n")
        f.write(content_feats.to_string(index=False))
    print("  Results saved to batch_results.txt")


if __name__ == "__main__":
    main()
