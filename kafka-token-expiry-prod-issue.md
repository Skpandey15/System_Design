# Kafka Kickout + Token Expiry: Distributed Causality & Resolution Design

## Problem Statement

10 microservices are exhibiting **two correlated failures** in Datadog logs:
- `Kafka consumer kickout` (consumer group rebalance / session timeout)
- `Token expired` (auth token lifecycle issue)

**Core challenge:** Logs are scattered across 10 components with no clear causal chain —
we cannot tell which failure triggered the cascade.

---

## 1. Root Cause Hypothesis Map

```
Two possible causal chains exist:

CHAIN A — Token Expiry causes Kafka Kickout
─────────────────────────────────────────────────────────────────────
  Token Expires
      │
      ▼
  Service can't re-authenticate to broker / schema registry
      │
      ▼
  Consumer stalls / stops polling Kafka
      │
      ▼
  Kafka broker sees no heartbeat beyond session.timeout.ms
      │
      ▼
  Consumer KICKED OUT → group rebalance triggered
      │
      ▼
  All 10 consumers pause during rebalance → cascade lag spike
─────────────────────────────────────────────────────────────────────

CHAIN B — Kafka Kickout causes Token Expiry appearance
─────────────────────────────────────────────────────────────────────
  Kafka rebalance (infra issue, broker restart, network blip)
      │
      ▼
  Consumer restart / pod eviction
      │
      ▼
  In-memory token cache LOST (token not persisted)
      │
      ▼
  Service restarts cold → token appears "expired" or missing
      │
      ▼
  Auth failure logged as "token expired"
─────────────────────────────────────────────────────────────────────
```

---

## 2. Immediate Investigation Strategy (Right Now in Datadog)

### Step 1 — Find the TRUE first event using timestamp ordering

```
Datadog Log Query to identify the earliest failure:

  service:(svc-A OR svc-B OR ... svc-J)
  (status:error "token expired" OR "kickout" OR "rebalance" OR "session.timeout")
  @timestamp:[now-2h TO now]

  → Sort by: timestamp ASC
  → Group by: service
  → Look at: FIRST error timestamp across ALL 10 services
```

**Key insight:** The service with the earliest error timestamp is the origin.

### Step 2 — Correlate via Kafka Consumer Group Lag metric

```
Datadog Metric Query:
  kafka.consumer_group.lag{consumer_group:*}

  → Plot as timeseries
  → Look for the LAG SPIKE — it will precede most log errors
  → The consumer group that spiked FIRST = origin service
```

### Step 3 — Check token expiry time vs. kickout time

```
In Datadog:
  → Filter: "token expired"
  → Extract: token_issued_at, token_expires_at from structured logs
  → Compare: token_expires_at vs first Kafka kickout timestamp

  If token_expires_at < kafka_kickout_timestamp → Chain A (Token caused kickout)
  If token_expires_at > kafka_kickout_timestamp → Chain B (Kickout caused token loss)
```

---

## 3. Architectural Solution: Event Causality Service

The long-term fix requires a dedicated **Event Correlation & Causality Layer**.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     PRODUCTION ENVIRONMENT                          │
│                                                                     │
│  ┌──────┐  ┌──────┐  ┌──────┐  ...  ┌──────┐                      │
│  │ Svc A│  │ Svc B│  │ Svc C│       │Svc J │   (10 microservices) │
│  └──┬───┘  └──┬───┘  └──┬───┘       └──┬───┘                      │
│     │         │          │              │                           │
│     └─────────┴──────────┴──────────────┘                          │
│                          │                                          │
│              Structured logs with:                                  │
│              - correlation_id (shared across request)               │
│              - causation_id   (parent event ID)                     │
│              - component_name                                       │
│              - event_type: [TOKEN_EXPIRY | KAFKA_KICKOUT | ...]     │
│              - logical_timestamp (Lamport clock / vector clock)     │
│                          │                                          │
│                          ▼                                          │
│              ┌───────────────────────┐                              │
│              │   Datadog Log Pipeline│                              │
│              │   (Parsing + Enrichment)                             │
│              └───────────┬───────────┘                              │
│                          │                                          │
│           ┌──────────────┴──────────────┐                          │
│           ▼                             ▼                           │
│  ┌────────────────┐          ┌─────────────────────┐               │
│  │ Causality      │          │  Alert Deduplication │               │
│  │ Timeline Board │          │  (suppress child     │               │
│  │ (root cause    │          │   alerts if root     │               │
│  │  highlighted)  │          │   alert exists)      │               │
│  └────────────────┘          └─────────────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Structured Logging Contract (All 10 Services Must Follow)

Every service MUST emit logs in this schema:

```json
{
  "timestamp": "2026-06-23T10:45:00.123Z",
  "service": "order-consumer",
  "correlation_id": "req-abc-123",
  "causation_id":   "evt-xyz-999",
  "trace_id":       "dd-trace-id-from-opentelemetry",
  "event_type":     "KAFKA_CONSUMER_KICKOUT",
  "severity":       "ERROR",
  "kafka": {
    "consumer_group": "order-group",
    "topic":          "orders",
    "partition":      3,
    "session_timeout_ms": 30000,
    "last_poll_ms_ago":   35000
  },
  "token": {
    "issued_at":   "2026-06-23T09:00:00Z",
    "expires_at":  "2026-06-23T10:00:00Z",
    "status":      "VALID | EXPIRED | MISSING"
  },
  "root_cause_hint": "token_status=EXPIRED precedes this event by 300ms"
}
```

**Why `causation_id`?**
When Service A's token expires and triggers Service B's Kafka kickout,
Service B logs `causation_id = Service A's token expiry event ID`.
This creates a **traceable causal chain** across services.

---

## 5. Fix: Token Lifecycle Manager (Proactive Refresh)

The key fix to break Chain A: **never let a token expire while a consumer is running.**

```
┌──────────────────────────────────────────────────────┐
│              Token Lifecycle Manager                  │
│                                                       │
│   token_ttl = 3600s                                   │
│   refresh_at = token_ttl * 0.75  → refresh at 2700s  │
│                                                       │
│   ┌────────────────────────────────────────────┐      │
│   │  Background Thread / Sidecar               │      │
│   │                                            │      │
│   │  every 60s:                                │      │
│   │    if (now > token.issued_at + refresh_at) │      │
│   │      → call auth service for new token     │      │
│   │      → hot-swap token in consumer config   │      │
│   │      → NO consumer restart needed          │      │
│   │      → emit TOKEN_REFRESHED event to log   │      │
│   └────────────────────────────────────────────┘      │
│                                                       │
│   If refresh FAILS:                                   │
│     → pause consumer (do NOT let it kickout)          │
│     → emit CONSUMER_PAUSED + TOKEN_REFRESH_FAILED     │
│     → retry with exponential backoff (1s,2s,4s,8s)   │
│     → circuit breaker after 5 failures → alert ops    │
└──────────────────────────────────────────────────────┘
```

---

## 6. Fix: Kafka Consumer Resilience (Break Chain B)

```
Consumer Configuration Tuning:
─────────────────────────────────────────────────────
  session.timeout.ms       = 45000   (was: 10000)
  heartbeat.interval.ms    = 3000    (1/3 of session timeout)
  max.poll.interval.ms     = 300000  (5 min, for slow processing)
  max.poll.records         = 50      (reduce batch size to poll faster)

  → Heartbeat runs in SEPARATE thread from poll loop
  → Even if processing is slow, heartbeat keeps consumer alive
─────────────────────────────────────────────────────

On Consumer Kickout Event:
  1. Log: KAFKA_KICKOUT with full context (last_poll, token_status)
  2. Pause: consumer.pause() — do NOT close
  3. Token check: verify token is valid BEFORE rejoining group
  4. Resume: consumer.resume() after token confirmed valid
  5. Rejoin: allow broker-initiated rebalance to complete
```

---

## 7. Datadog Monitors & Alert Correlation

### Monitor 1: Token Expiry (Root Cause Detector)
```
Query:  logs("event_type:TOKEN_EXPIRY").count().by("service").last("5m") > 1
Alert:  "POSSIBLE ROOT CAUSE: Token expired in {service}"
Priority: P1
Tag: root_cause_candidate
```

### Monitor 2: Kafka Kickout (Effect Detector)
```
Query:  logs("event_type:KAFKA_CONSUMER_KICKOUT").count().by("service").last("5m") > 1
Alert:  "Kafka kickout in {service} — check if token expiry is root cause"
Priority: P2
Tag: possible_cascade
```

### Monitor 3: Cascade Detector (Composite)
```
Composite Alert:
  IF Monitor1 (token_expiry) fires WITHIN 5 min of Monitor2 (kafka_kickout)
  THEN:
    → Suppress individual P2 alerts
    → Fire single P1: "Token expiry cascade detected across N services"
    → Include: first_affected_service, event_timeline_link
```

### Datadog Dashboard Query — Causality Timeline
```
# Single query to see ordered event chain
logs("service:* (event_type:TOKEN_EXPIRY OR event_type:KAFKA_CONSUMER_KICKOUT)")
  .groupby(["service","event_type"])
  .timeseries(interval=30s)

# Pivot table: rows=service, cols=time, value=first_event_type
```

---

## 8. Distributed Tracing with OpenTelemetry

Add OpenTelemetry spans to create a **single trace across all 10 services**:

```
Trace: "prod-incident-kafka-token-cascade"
  │
  ├── Span: auth-service.token_refresh [FAILED] ← ROOT CAUSE
  │     attributes:
  │       token.expires_at: "10:00:00"
  │       token.refresh_attempted_at: "09:59:30"
  │       error: "auth-server timeout"
  │
  ├── Span: order-consumer.kafka_poll [STALLED] ← EFFECT 1
  │     parent_span: auth-service.token_refresh
  │     attributes:
  │       kafka.last_poll_ms_ago: 35000
  │       kafka.session_timeout_ms: 30000
  │
  ├── Span: payment-consumer.kafka_rebalance [TRIGGERED] ← EFFECT 2
  │     parent_span: order-consumer.kafka_poll
  │
  └── ... (cascades to all 10 services)
```

In Datadog APM, this renders as a **flame graph** showing exact cascade order.

---

## 9. Implementation Roadmap

```
PHASE 1 — TODAY (Stop the bleeding)
─────────────────────────────────────────────────────
  □ Manually check Datadog: sort logs by timestamp ASC to find origin
  □ Check Kafka consumer group lag metric for the first spike
  □ Compare token.expires_at vs kafka_kickout timestamp
  □ Manual token rotation for affected services
  □ Tune: session.timeout.ms = 45000, heartbeat.interval.ms = 3000

PHASE 2 — THIS WEEK (Prevent recurrence)
─────────────────────────────────────────────────────
  □ Add correlation_id + causation_id + event_type to all 10 services
  □ Implement Token Lifecycle Manager (proactive refresh at 75% TTL)
  □ Add TOKEN_EXPIRY and KAFKA_KICKOUT structured log events
  □ Create Datadog composite alert monitor

PHASE 3 — THIS MONTH (Full observability)
─────────────────────────────────────────────────────
  □ Deploy OpenTelemetry collector sidecar across all 10 services
  □ Instrument Kafka consumer spans with parent-child relationships
  □ Build Datadog causality timeline dashboard
  □ Implement consumer pause-on-token-failure (not kickout)
  □ Add circuit breaker on auth service calls from consumers
```

---

## 10. Quick Reference: How to Read the Datadog Logs RIGHT NOW

```
Step 1 — Find origin service:
  Search: "token expired" OR "consumer kicked out" OR "rebalance"
  Sort by: oldest first
  Result: service with EARLIEST timestamp = likely origin

Step 2 — Confirm with Kafka lag:
  Metrics → kafka.consumer_group.lag
  Find: which consumer group's lag spiked FIRST

Step 3 — Determine chain direction:
  IF token_expiry_time < kafka_kickout_time:
    → Fix: Token Lifecycle Manager (proactive refresh)
  IF kafka_kickout_time < token_expiry_time:
    → Fix: Consumer resilience + persistent token cache

Step 4 — Suppress noise:
  All subsequent alerts in other 9 services = cascade effects
  Focus recovery on the ORIGIN service first
```

---

## Summary

| Problem | Root Cause Pattern | Fix |
|---|---|---|
| Token expires → Kafka kickout | Chain A | Proactive token refresh at 75% TTL |
| Kafka kickout → token lost | Chain B | Persist token outside consumer memory |
| Can't find which happened first | No causation_id in logs | Add structured logs with `causation_id` |
| 10 services all alerting | No composite monitor | Datadog cascade detector monitor |
| No single timeline view | No distributed tracing | OpenTelemetry + Datadog APM |
