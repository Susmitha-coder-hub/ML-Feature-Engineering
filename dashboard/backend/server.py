#!/usr/bin/env python3
"""
Dashboard Backend — FastAPI + WebSockets
Consumes from feature-store and flink-metrics Kafka topics,
serves a REST API and pushes real-time updates to connected browsers.
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from threading import Thread, Lock
from typing import Set

import httpx
import uvicorn
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("Dashboard")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS     = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
FEATURE_TOPIC     = os.environ.get("FEATURE_STORE_TOPIC", "feature-store")
METRICS_TOPIC     = os.environ.get("FLINK_METRICS_TOPIC", "flink-metrics")
FLINK_JM_URL      = os.environ.get("FLINK_JOB_MANAGER_URL", "http://flink-jobmanager:8081")
PORT              = int(os.environ.get("DASHBOARD_PORT", "8080"))

# ── In-Memory State ───────────────────────────────────────────────────────────
_lock = Lock()

# {entity_id: {feature_name: {"value": ..., "computed_at": ..., "received_at": float}}}
feature_store: dict = defaultdict(dict)

pipeline_metrics = {
    "late_events_dropped": 0,
    "total_features_computed": 0,
    "last_watermark_ts": None,      # ISO string
    "watermark_lag_seconds": None,  # float
    "last_update": None,
    "flink_job_status": "UNKNOWN",
}

# ── Kafka Consumer Thread ─────────────────────────────────────────────────────

def kafka_consumer_thread(loop: asyncio.AbstractEventLoop, broadcast_fn):
    conf = {
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": "dashboard-cg",
        "auto.offset.reset": "latest",  # show live data
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([FEATURE_TOPIC, METRICS_TOPIC])
    log.info("Kafka consumer subscribed to: %s, %s", FEATURE_TOPIC, METRICS_TOPIC)

    while True:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("Kafka error: %s", msg.error())
            continue

        try:
            raw = msg.value().decode("utf-8")
            data = json.loads(raw)
        except Exception as e:
            log.warning("Failed to parse message: %s", e)
            continue

        topic = msg.topic()
        if topic == FEATURE_TOPIC:
            _handle_feature(data)
            asyncio.run_coroutine_threadsafe(broadcast_fn(data), loop)
        elif topic == METRICS_TOPIC:
            _handle_metrics(data)


def _handle_feature(data: dict):
    entity_id    = data.get("entity_id", "")
    feature_name = data.get("feature_name", "")
    if not entity_id or not feature_name:
        return
    with _lock:
        feature_store[entity_id][feature_name] = {
            "value":       data.get("feature_value"),
            "computed_at": data.get("computed_at"),
            "received_at": time.time(),
        }
        pipeline_metrics["total_features_computed"] += 1
        pipeline_metrics["last_update"] = datetime.now(timezone.utc).isoformat()


def _handle_metrics(data: dict):
    with _lock:
        if "late_events_dropped" in data:
            pipeline_metrics["late_events_dropped"] += data["late_events_dropped"]
        if "watermark_ts" in data:
            pipeline_metrics["last_watermark_ts"] = data["watermark_ts"]
            try:
                wm_dt = datetime.fromisoformat(data["watermark_ts"].replace("Z", "+00:00"))
                lag = (datetime.now(timezone.utc) - wm_dt).total_seconds()
                pipeline_metrics["watermark_lag_seconds"] = round(lag, 1)
            except Exception:
                pass


# ── Flink Job Manager Polling ─────────────────────────────────────────────────

async def poll_flink_status():
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                resp = await client.get(f"{FLINK_JM_URL}/jobs/overview")
                if resp.status_code == 200:
                    jobs = resp.json().get("jobs", [])
                    if jobs:
                        pipeline_metrics["flink_job_status"] = jobs[0].get("state", "UNKNOWN")
                    else:
                        pipeline_metrics["flink_job_status"] = "NO_JOBS"
            except Exception:
                pipeline_metrics["flink_job_status"] = "UNREACHABLE"
            await asyncio.sleep(10)


# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(title="ML Pipeline Dashboard")

# Serve static frontend files
frontend_dir = Path(__file__).parent.parent / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, data: dict):
        dead = set()
        for ws in self.active:
            try:
                await ws.send_json({"type": "feature_update", "data": data})
            except Exception:
                dead.add(ws)
        self.active -= dead

manager = ConnectionManager()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
async def root():
    index_path = frontend_dir / "index.html"
    if index_path.exists():
        return HTMLResponse(index_path.read_text())
    return HTMLResponse("<h1>Dashboard loading…</h1>")


@app.get("/api/features/{entity_id}")
async def get_features(entity_id: str):
    with _lock:
        features = dict(feature_store.get(entity_id, {}))
    if not features:
        return JSONResponse({"entity_id": entity_id, "features": {}, "found": False})
    return {"entity_id": entity_id, "features": features, "found": True}


@app.get("/api/entities")
async def list_entities():
    with _lock:
        entities = list(feature_store.keys())
    return {"entities": sorted(entities)}


@app.get("/api/metrics")
async def get_metrics():
    with _lock:
        m = dict(pipeline_metrics)
    # Add feature freshness for two key features
    freshness = {}
    with _lock:
        for entity_id, feats in feature_store.items():
            for fname in ["click_rate", "engagement_rate"]:
                if fname in feats:
                    age = time.time() - feats[fname]["received_at"]
                    freshness[f"{entity_id}:{fname}"] = round(age, 1)
    m["feature_freshness_samples"] = dict(list(freshness.items())[:10])
    return m


@app.get("/api/features")
async def get_all_features():
    with _lock:
        result = {k: dict(v) for k, v in feature_store.items()}
    return {"total_entities": len(result), "data": result}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    log.info("WebSocket client connected. Total: %d", len(manager.active))
    try:
        # Send current state on connect
        with _lock:
            snapshot = {k: dict(v) for k, v in feature_store.items()}
            metrics  = dict(pipeline_metrics)
        await websocket.send_json({"type": "snapshot", "features": snapshot, "metrics": metrics})

        while True:
            # Keep connection alive + push metrics every 5s
            await asyncio.sleep(5)
            with _lock:
                metrics = dict(pipeline_metrics)
            await websocket.send_json({"type": "metrics", "data": metrics})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        log.info("WebSocket client disconnected.")


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()

    async def broadcast_wrapper(data: dict):
        await manager.broadcast(data)

    # Start Kafka consumer in a background thread
    t = Thread(
        target=kafka_consumer_thread,
        args=(loop, broadcast_wrapper),
        daemon=True,
    )
    t.start()

    # Start Flink status polling coroutine
    asyncio.create_task(poll_flink_status())
    log.info("Dashboard startup complete. Listening on port %d", PORT)


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, log_level="info")
