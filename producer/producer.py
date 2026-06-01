#!/usr/bin/env python3
"""
Real-Time ML Feature Engineering Pipeline — Data Producer
Simulates realistic user interaction events with configurable late-event injection.
"""

import json
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
USER_EVENTS_TOPIC       = os.environ.get("USER_EVENTS_TOPIC", "user-events")
CONTENT_METADATA_TOPIC  = os.environ.get("CONTENT_METADATA_TOPIC", "content-metadata")
TIME_ACCELERATION       = float(os.environ.get("TIME_ACCELERATION", "60"))
LATE_EVENT_RATIO        = float(os.environ.get("LATE_EVENT_RATIO", "0.07"))
LATE_MIN_SECONDS        = int(os.environ.get("LATE_EVENT_DELAY_MIN_SECONDS", "35"))
LATE_MAX_SECONDS        = int(os.environ.get("LATE_EVENT_DELAY_MAX_SECONDS", "90"))
NUM_USERS               = int(os.environ.get("NUM_USERS", "20"))
NUM_CONTENT             = int(os.environ.get("NUM_CONTENT", "50"))

# Canonical user IDs so submission.json can reference them deterministically
CANONICAL_USER_ID    = "user_001"
CANONICAL_CONTENT_ID = "content_001"

# ── Data Schemas ─────────────────────────────────────────────────────────────

CATEGORIES  = ["sci-fi", "action", "drama", "comedy", "documentary", "horror",
                "romance", "thriller", "animation", "sports"]
EVENT_TYPES = ["view", "click", "like", "share", "skip"]

@dataclass
class UserArchetype:
    name: str
    session_prob: float        # probability of being active in any given tick
    events_per_session: tuple  # (min, max) events per session
    preferred_categories: List[str]
    event_weights: Dict[str, float]  # event_type → relative weight

ARCHETYPES = [
    UserArchetype(
        name="binge_watcher",
        session_prob=0.8,
        events_per_session=(10, 30),
        preferred_categories=["sci-fi", "drama", "action"],
        event_weights={"view": 0.6, "click": 0.2, "like": 0.1, "share": 0.05, "skip": 0.05},
    ),
    UserArchetype(
        name="news_scanner",
        session_prob=0.5,
        events_per_session=(3, 10),
        preferred_categories=["documentary", "sports"],
        event_weights={"view": 0.3, "click": 0.4, "like": 0.05, "share": 0.15, "skip": 0.1},
    ),
    UserArchetype(
        name="casual_browser",
        session_prob=0.3,
        events_per_session=(1, 5),
        preferred_categories=CATEGORIES,
        event_weights={"view": 0.25, "click": 0.25, "like": 0.1, "share": 0.05, "skip": 0.35},
    ),
    UserArchetype(
        name="power_liker",
        session_prob=0.6,
        events_per_session=(5, 15),
        preferred_categories=["comedy", "animation", "romance"],
        event_weights={"view": 0.3, "click": 0.2, "like": 0.3, "share": 0.15, "skip": 0.05},
    ),
]

# ── Helpers ──────────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for key %s: %s", msg.key(), err)


def make_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    }
    return Producer(conf)


def sim_time_to_wall(sim_ts: datetime) -> datetime:
    """No conversion needed — sim_ts IS the event timestamp we embed."""
    return sim_ts


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Content Metadata ──────────────────────────────────────────────────────────

def build_content_catalog(n: int) -> List[Dict]:
    random.seed(42)
    items = []
    creator_ids = [f"creator_{i:03d}" for i in range(1, 11)]
    for i in range(1, n + 1):
        cid = f"content_{i:03d}"
        if i == 1:
            cid = CANONICAL_CONTENT_ID  # ensure canonical exists
        items.append({
            "content_id": cid,
            "category": random.choice(CATEGORIES),
            "creator_id": random.choice(creator_ids),
            "publish_timestamp": iso(
                datetime(2024, 1, 1, tzinfo=timezone.utc)
                + timedelta(days=random.randint(0, 365))
            ),
        })
    return items


def publish_content_metadata(producer: Producer, catalog: List[Dict]):
    log.info("Publishing %d content metadata records…", len(catalog))
    for item in catalog:
        producer.produce(
            topic=CONTENT_METADATA_TOPIC,
            key=item["content_id"].encode(),
            value=json.dumps(item).encode(),
            on_delivery=delivery_report,
        )
    producer.flush()
    log.info("Content metadata published.")


# ── User Generation ───────────────────────────────────────────────────────────

def build_users(n: int) -> List[Dict]:
    users = []
    for i in range(1, n + 1):
        uid = f"user_{i:03d}"
        if i == 1:
            uid = CANONICAL_USER_ID
        archetype = random.choice(ARCHETYPES)
        users.append({"user_id": uid, "archetype": archetype})
    return users


# ── Event Generation ──────────────────────────────────────────────────────────

def generate_event(user: Dict, content_catalog: List[Dict], sim_now: datetime) -> Dict:
    archetype: UserArchetype = user["archetype"]
    # pick content biased toward preferred categories
    preferred = [c for c in content_catalog if c["category"] in archetype.preferred_categories]
    pool = preferred if preferred and random.random() < 0.7 else content_catalog
    content = random.choice(pool)

    event_type = random.choices(
        list(archetype.event_weights.keys()),
        weights=list(archetype.event_weights.values()),
    )[0]

    # dwell_time: views/clicks linger longer
    if event_type in ("view", "click"):
        dwell_ms = random.randint(5_000, 300_000)
    else:
        dwell_ms = random.randint(1_000, 30_000)

    return {
        "user_id": user["user_id"],
        "content_id": content["content_id"],
        "event_type": event_type,
        "dwell_time_ms": dwell_ms,
        "timestamp": iso(sim_now),
    }


def maybe_make_late(event: Dict, sim_now: datetime) -> Dict:
    """With LATE_EVENT_RATIO probability, backdate the event timestamp."""
    if random.random() < LATE_EVENT_RATIO:
        delay = random.randint(LATE_MIN_SECONDS, LATE_MAX_SECONDS)
        late_ts = sim_now - timedelta(seconds=delay)
        event = dict(event)
        event["timestamp"] = iso(late_ts)
        log.debug("Late event injected: %s (delay=%ds)", event["user_id"], delay)
    return event


# ── Main Loop ─────────────────────────────────────────────────────────────────

def run():
    log.info("Producer starting — bootstrap=%s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("Config: TIME_ACCELERATION=%.0fx, LATE_RATIO=%.0f%%, "
             "LATE_DELAY=%d–%ds, USERS=%d, CONTENT=%d",
             TIME_ACCELERATION, LATE_EVENT_RATIO * 100,
             LATE_MIN_SECONDS, LATE_MAX_SECONDS, NUM_USERS, NUM_CONTENT)

    producer = make_producer()
    content_catalog = build_content_catalog(NUM_CONTENT)
    users = build_users(NUM_USERS)

    # Publish metadata first
    publish_content_metadata(producer, content_catalog)

    # Sim clock starts at a round hour in the past so windows fire quickly
    sim_start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    wall_start = time.monotonic()

    total_events = 0
    late_events  = 0
    tick_interval_real = 0.5  # seconds of real time between ticks

    log.info("Starting event loop. Sim start: %s", iso(sim_start))

    # Touch alive-file for healthcheck
    open("/tmp/producer_alive", "w").close()

    while True:
        wall_elapsed = time.monotonic() - wall_start
        sim_elapsed  = wall_elapsed * TIME_ACCELERATION
        sim_now      = sim_start + timedelta(seconds=sim_elapsed)

        batch_events = []
        for user in users:
            archetype: UserArchetype = user["archetype"]
            if random.random() < archetype.session_prob * tick_interval_real:
                n_events = random.randint(*archetype.events_per_session)
                # spread events within the tick window
                for _ in range(n_events):
                    jitter = random.uniform(0, tick_interval_real * TIME_ACCELERATION)
                    evt_time = sim_now - timedelta(seconds=jitter)
                    evt = generate_event(user, content_catalog, evt_time)
                    original_ts = evt["timestamp"]
                    evt = maybe_make_late(evt, evt_time)
                    if evt["timestamp"] != original_ts:
                        late_events += 1
                    batch_events.append(evt)

        for evt in batch_events:
            producer.produce(
                topic=USER_EVENTS_TOPIC,
                key=evt["user_id"].encode(),
                value=json.dumps(evt).encode(),
                on_delivery=delivery_report,
            )
        producer.poll(0)

        total_events += len(batch_events)
        if total_events % 500 < len(batch_events):
            log.info("sim_time=%s | total=%d events | late=%d (%.1f%%)",
                     iso(sim_now), total_events, late_events,
                     100 * late_events / max(1, total_events))
            # Refresh alive file
            open("/tmp/producer_alive", "w").close()

        time.sleep(tick_interval_real)


if __name__ == "__main__":
    # Brief startup delay to let Kafka be fully ready
    time.sleep(5)
    run()
