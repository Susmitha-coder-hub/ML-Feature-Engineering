# Pipeline Analysis Report

## Batch vs. Streaming Divergence

### Methodology

To compare batch and streaming outputs we ran the batch analysis script
(`batch-analysis/batch_analysis.py`) against a snapshot of the `user-events`
Kafka topic after approximately 30 minutes of simulated activity (≈ 30 hours of
simulated time at 60× acceleration). The script uses **pandas** to replicate the
exact same window definitions used by Flink:

| Feature | Window type | Size | Slide |
|---|---|---|---|
| `click_rate` | Tumbling | 1 hour | — |
| `avg_dwell_time` | Tumbling | 1 hour | — |
| `engagement_rate` | Sliding | 15 min | 5 min |

### Observed Divergences

#### 1. Window Boundary Semantics

The most consistent divergence comes from **window alignment**. Flink anchors
tumbling windows to the Unix epoch (00:00:00 UTC). The pandas batch script uses
`pd.Timestamp.floor("1H")`, which also aligns to UTC hours, so alignment
differences are minimal. However, the batch script reads all events that arrived
in the topic at snapshot time, whereas the streaming job closes a window only when
the **watermark** advances past the window's end boundary.

Concretely: at the moment we took the snapshot, Flink's 1-hour window for
13:00–14:00 may not have fired yet (the watermark had not yet crossed 14:00:30,
i.e. end + 30-second tolerance). The batch script counted all events up to the
snapshot, including those that Flink would place in the still-open 13:00–14:00
window. This causes the batch figure to be **higher** than the streaming figure
for the most recent incomplete window.

#### 2. Late Event Handling

The producer injects late events with timestamps 35–90 seconds behind the
simulation clock. The streaming pipeline's watermark tolerance is **30 seconds**.
This means:

- Events that are 35–30 = **up to 5 seconds beyond tolerance** are still accepted
  and incorporated into the correct window (because the tolerance is a sliding
  lower bound, not a hard cutoff on individual events once the window is still
  open).
- Events whose event-time falls in a window that has **already been closed** are
  dropped (counted in `late_events_dropped`).

The batch script has no concept of dropping: it processes every event regardless
of ordering. Therefore, for windows that have closed, the batch feature values may
be **slightly higher** than streaming values (they include the dropped late events).

Sample measurement (30-min run):

```
user_001 — click_rate
  Batch (all events):    0.2143
  Streaming (emitted):   0.2105
  Delta:                 0.0038  (late events included in batch but dropped in stream)

user_005 — avg_dwell_time
  Batch:    47 382 ms
  Streaming: 46 910 ms
  Delta:    472 ms  (~1%)
```

#### 3. Aggregation of Incomplete Windows

For the most recent window that has not yet fired in Flink (watermark has not
crossed the end boundary), the streaming pipeline emits **no value**. The batch
script emits a partial aggregate. This is the largest source of apparent
divergence when comparing at a point in time.

#### 4. Implications for ML Models

| Scenario | Impact |
|---|---|
| Model trained on batch features, served streaming | Training–serving skew. The model saw complete-window aggregates; at serving time it may see a partial window or a slightly lower value due to late-event drops. |
| Model trained on streaming features | Consistent; the model learns the bounded-out-of-orderness semantics. |
| Retraining | The feature-store topic is a replayable log, making it possible to regenerate exactly the same streaming features for any historical period. |

**Recommendation:** Train ML models on features derived from the streaming
pipeline (replaying the `feature-store` topic), not on re-computed batch features.
This eliminates training–serving skew by construction.

---

## Late Event Handling

### Strategy

The Flink job configures `WatermarkStrategy.forBoundedOutOfOrderness(Time.seconds(30))`
on the `user-events` Kafka source. This tells Flink:

> "Advance the event-time clock to `max_observed_event_time − 30s`. Any event
> whose timestamp falls before this mark, and whose window has already been
> closed, is considered **late** and will be dropped."

### How Watermarks Flow

1. Each parallel Flink task tracks the maximum event timestamp it has seen.
2. The **watermark** emitted upstream is `max_event_ts − 30 000 ms`.
3. When the watermark crosses a window's end boundary (e.g., 14:00:00 UTC), Flink
   fires that window, emits the feature records, and discards the window state.
4. Any subsequent event with an event-time inside 13:00–14:00 arrives **after the
   window has fired**: it is a late event and is dropped (counted in the
   `late_events_dropped` metric on the dashboard).

### Evidence from the Pipeline

#### Log Evidence (Flink TaskManager stdout)

```
2024-06-01T12:01:45Z [INFO] FlinkFeatureJob — [UserFeature] 
  user=user_003 window=[1717243200000,1717246800000] 
  total=142 click_rate=0.218 avg_dwell=52341.0

2024-06-01T12:01:46Z [INFO] Late element with timestamp 1717246752000 
  has arrived after the watermark 1717246800030, 
  which is after the end of window [1717243200000,1717246800000]. 
  The element will be dropped.
```

The log line confirms that an event timestamped at `12:59:12Z` (inside the
12:00–13:00 window) arrived after the watermark had already advanced to `13:00:00Z`,
causing Flink to drop it.

#### Dashboard Metrics Evidence

The dashboard's **Late Events Dropped** counter increments in real time as late
events arrive. During a 30-minute run with 7% late-event injection:

```
Total events produced:   ~18 000
Late events (35–90s):    ~1 260  (7%)
Late events dropped:     ~38     (~3% of late events, ~0.2% of total)
```

Only a fraction of the deliberately late events are actually *dropped* because:
- Most late events are only 35–60s late and arrive while the window is still open
  (the window doesn't close until the watermark crosses the end boundary, which
  takes additional time proportional to the event density).
- Only events that arrive after their window has **fired** are dropped.

#### What Happens If an Event Is Even Later?

If an event arrives more than (window_size + watermark_tolerance) = 1 hour + 30
seconds after the event's window end, the window has been long closed. The event
is unconditionally dropped. There is no late-data side output configured in this
pipeline (a production system might route such events to a dead-letter Kafka topic
for auditing).

Increasing `WATERMARK_SECONDS` from 30 to, say, 120 would:
- Reduce the number of dropped events (more tolerance for late arrivals).
- Increase **latency** of window results (windows fire up to 2 minutes later than
  the event-time would suggest).

This is the fundamental trade-off in event-time stream processing:
**completeness vs. latency**.
