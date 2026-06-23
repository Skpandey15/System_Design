# AI-Powered RCA System: Architecture Deep Dive

## Your Architecture (Reconstructed)

```
API Gateway
     │
     ├──────────────────────────────────┐
     ▼                                  ▼
Log Collector                   Incident Query API
     │
     ▼
Normalizer Service
     │
     ▼
Embedding Service (BGE-M3 / E5 / OpenAI)
     │
     ▼
Semantic Event Mapper
     │
     ▼
Kafka Topic: semantic-events
     │
     ▼
Ontology Service (Neo4j)
     │
     ▼
Correlation Engine
     │
     ▼
LangGraph Orchestrator
     │
     ├──────────────────────────────────────────────┐
     ▼                 ▼                            ▼
Timeline Agent     Graph Agent              Similarity Agent
                                                   │
                                               Qdrant VDB
     └─────────────────┬────────────────────────────┘
                       ▼
                 Reasoning Agent
                       │
                  GPT-4o / Claude
                       │
                  RCA Generator
```

---

## Strengths of This Design

| Component | Why It's Right |
|---|---|
| Embedding Service | Semantic understanding catches errors that differ in wording but mean the same thing |
| Neo4j Ontology | Service dependency graph enables true blast-radius analysis |
| LangGraph multi-agent | Separates concerns — timeline vs graph vs similarity are orthogonal reasoning tasks |
| Qdrant for similarity | Historical incident recall prevents re-diagnosing known issues |
| LLM as final reasoner | Synthesizes structured signals into human-readable RCA |

---

## Critical Gaps (Must Fix)

### Gap 1 — NO Causal Clock (the original problem is still unsolved)

**Problem:** Distributed systems have clock skew (NTP drift ±100ms to ±500ms).
Sorting by `timestamp ASC` in Datadog is UNRELIABLE for causality.

**Example:**
```
Service A logs token_expiry  at T=10:00:00.100  (clock ahead by 200ms)
Service B logs kafka_kickout at T=09:59:59.950  (clock normal)

Sort by timestamp → Kafka kickout appears FIRST
Reality         → Token expiry happened first
```

**Fix: Add a Vector Clock Injector before the Normalizer**

```
Logs (with wall-clock timestamps)
          │
          ▼
┌─────────────────────────┐
│  Vector Clock Injector  │
│                         │
│  Assigns logical clock  │
│  value to each event:   │
│                         │
│  VC(A) = {A:3, B:1}    │
│  VC(B) = {A:2, B:5}    │
│                         │
│  Causal rule:           │
│  If VC(A) < VC(B)      │
│  → A happened-before B │
└─────────────────────────┘
          │
          ▼
    Normalizer Service
```

For the Kafka/token case specifically, add `causation_id` at the source:
```json
{
  "event_type": "KAFKA_CONSUMER_KICKOUT",
  "causation_id": "<token_expiry_event_id>",
  "vector_clock": {"auth-svc": 3, "order-consumer": 1}
}
```

---

### Gap 2 — No Alert Deduplication Before Agents

**Problem:** 10 services cascade → 10 separate events all hit LangGraph.
Each agent run is expensive (LLM calls). You'll trigger 10 RCA jobs for 1 root cause.

**Fix: Add a Deduplication + Grouping Layer**

```
Correlation Engine
        │
        ▼
┌──────────────────────────────────┐
│    Alert Deduplication Layer     │
│                                  │
│  Rules:                          │
│  1. Same error type within 5min  │
│     + Same Kafka consumer group  │
│     → GROUP into 1 incident      │
│                                  │
│  2. Error count > 3 services     │
│     within 2min window           │
│     → Mark as CASCADE_INCIDENT   │
│                                  │
│  3. Emit ONE grouped event with: │
│     - affected_services: [...]   │
│     - first_event_timestamp      │
│     - root_candidate: svc-X      │
└──────────────────────────────────┘
        │
        ▼
  LangGraph Orchestrator  ← single invocation, not 10
```

---

### Gap 3 — Incident Query API is Disconnected

**Problem:** The Incident Query API has no shown read path.
What does it query? Kafka? Neo4j? Qdrant?
This needs to be an **aggregated read model**.

**Fix: Add an Incident Store**

```
                RCA Generator
                      │
                      ▼
         ┌────────────────────────┐
         │      Incident Store    │
         │   (PostgreSQL / Redis) │
         │                        │
         │  incident_id           │
         │  root_cause_service    │
         │  causal_chain: [...]   │
         │  confidence_score: 0.87│
         │  rca_summary: "..."    │
         │  status: OPEN/RESOLVED │
         │  created_at            │
         └───────────┬────────────┘
                     │
                     ▼
           Incident Query API   ← NOW has a real read model
```

---

### Gap 4 — No Feedback Loop (the system never learns)

**Problem:** RCA Generator produces output but it goes nowhere.
Next time the same Kafka+token pattern hits, Qdrant has no record of it.
The system re-diagnoses from scratch every time.

**Fix: RCA Feedback Pipeline**

```
RCA Generator
      │
      ├──────────────────────────────────────┐
      ▼                                      ▼
Incident Store                    ┌─────────────────────┐
                                  │   Learning Pipeline  │
                                  │                      │
                                  │  1. Embed RCA text   │
                                  │  2. Store in Qdrant  │
                                  │     with metadata:   │
                                  │     - pattern_type   │
                                  │     - root_cause     │
                                  │     - fix_applied    │
                                  │                      │
                                  │  3. Update Neo4j:    │
                                  │     Add edge:        │
                                  │     (TokenExpiry)    │
                                  │     -[CAUSES]->      │
                                  │     (KafkaKickout)   │
                                  │     {weight: +1}     │
                                  └─────────────────────┘
```

After 10 incidents, Neo4j knows with high confidence that
`TokenExpiry -[CAUSES {weight:10}]-> KafkaKickout`.
RCA becomes near-instant.

---

### Gap 5 — Embedding Service has the Same Token Problem

**Problem (ironic):** The Embedding Service calls OpenAI API with an auth token.
If that token expires, your RCA pipeline for the token-expiry problem...
itself fails due to token expiry.

**Fix: Embedding Service Resilience**

```
┌──────────────────────────────────────────────────┐
│              Embedding Service                    │
│                                                   │
│  Primary:   OpenAI text-embedding-3-large         │
│  Fallback:  BGE-M3 (local, no token needed)       │
│  Fallback2: E5-large-v2 (local)                   │
│                                                   │
│  Token Manager:                                   │
│    - Refresh at 75% TTL (same fix from design v1) │
│    - Health check every 60s                       │
│                                                   │
│  On token failure:                                │
│    → Failover to local model immediately          │
│    → No pipeline interruption                     │
│    → Alert: "Embedding degraded to local model"   │
└──────────────────────────────────────────────────┘
```

---

## Enhanced Architecture (Full)

```
                        API Gateway
                             │
          ┌──────────────────┴──────────────────┐
          ▼                                     ▼
   Log Collector                        Incident Query API
   (Datadog Forwarder /                        │
    Fluent Bit / OTel)                         │ reads from
          │                                    ▼
          ▼                            ┌───────────────┐
  Vector Clock Injector                │ Incident Store │
  (assign logical timestamps)          │  (Postgres)    │
          │                            └───────────────┘
          ▼
  Normalizer Service ◄──── Dead Letter Queue
  (schema validation,         (failed events
   field extraction)           → retry / alert)
          │
          ▼
  ┌───────────────────────────────────┐
  │        Embedding Cache            │
  │  (Redis, TTL=1h)                  │
  │  key: hash(normalized_event)      │
  │  value: embedding vector          │
  │  → avoid re-embedding duplicates  │
  └───────────────┬───────────────────┘
                  │ cache miss
                  ▼
  Embedding Service
  ┌──────────────────────┐
  │ Primary: OpenAI      │
  │ Fallback: BGE-M3     │ ← local, token-free
  │ Fallback: E5-large   │ ← local, token-free
  └──────────────────────┘
          │
          ▼
  Semantic Event Mapper
  (map embedding → event_type ontology labels)
          │
          ▼
  Kafka Topic: semantic-events
  ┌────────────────────────────────┐
  │  Partitioned by: service_name │
  │  Retention: 7 days            │
  │  Consumer group: rca-engine   │
  │  DLQ: semantic-events-dlq     │
  └────────────────────────────────┘
          │
          ▼
  ┌──────────────────────────────────────┐
  │      Alert Deduplication Layer       │
  │  - Group events by time window       │
  │  - Mark CASCADE_INCIDENT             │
  │  - Emit single grouped event         │
  └────────────────────┬─────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
  Ontology Service           Correlation Engine
  (Neo4j)                    (temporal + semantic
          │                   co-occurrence scoring)
          │                         │
          └────────────┬────────────┘
                       ▼
               LangGraph Orchestrator
               ┌───────────────────────────────────────┐
               │                                       │
               ▼            ▼               ▼          │
        Timeline Agent  Graph Agent   Similarity Agent │
        (causal order   (Neo4j        (Qdrant query    │
         via VC)         traversal)    top-k similar   │
                                       past incidents) │
               └───────────┬───────────────────────────┘
                           ▼
                   Reasoning Agent
                   ┌──────────────────────────────┐
                   │  Prompt:                     │
                   │  - Timeline: [...]           │
                   │  - Causal graph: [...]       │
                   │  - Similar incidents: [...]  │
                   │  - Confidence scores         │
                   └─────────────┬────────────────┘
                                 │
                         GPT-4o / Claude
                                 │
                         RCA Generator
                                 │
               ┌─────────────────┴──────────────────┐
               ▼                                     ▼
       Incident Store                    Learning Pipeline
       (write RCA result)                ┌──────────────────┐
               │                         │ Embed RCA text   │
               ▼                         │ → Qdrant (store) │
       Incident Query API                │ Update Neo4j     │
       (read by ops team)                │ edge weights     │
                                         └──────────────────┘
```

---

## Agent Responsibilities (Detailed)

### Timeline Agent
```
Input:  grouped incident events with vector clocks
Task:   reconstruct causal order using happened-before relation
Output: ordered event chain with confidence

Algorithm:
  1. Sort by vector clock (not wall clock)
  2. Identify first event (no happened-before predecessor)
  3. Build DAG of causality
  4. Return: [(event, service, vc_time, causal_rank), ...]

Prompt template:
  "Given these events in causal order:
   [1] T+0ms   auth-svc: TOKEN_EXPIRY  {token.expires_at: X}
   [2] T+312ms order-consumer: POLL_STALL {last_poll_ms_ago: 35000}
   [3] T+890ms order-consumer: KAFKA_KICKOUT
   [4] T+1200ms payment-consumer: REBALANCE_TRIGGERED
   Identify the root cause event and explain the cascade."
```

### Graph Agent
```
Input:  service names from incident
Task:   query Neo4j for:
          - service dependency edges
          - known causal pattern edges (learned from past RCAs)
          - Kafka consumer group memberships
Output: subgraph of affected components + causal_weight scores

Cypher query example:
  MATCH (root:Service)-[c:CAUSES*1..3]->(affected:Service)
  WHERE root.name = 'auth-service'
  AND c.pattern IN ['TOKEN_EXPIRY', 'KAFKA_KICKOUT']
  RETURN root, c, affected
  ORDER BY c.weight DESC
```

### Similarity Agent
```
Input:  embedding of current incident description
Task:   query Qdrant top-5 similar past incidents
Output: [(incident_id, similarity_score, root_cause, fix_applied), ...]

Qdrant query:
  collection: "rca_incidents"
  vector: embed("kafka kickout token expiry cascade 10 services")
  top_k: 5
  filter: {resolved: true}  ← only learn from resolved incidents

Use case:
  If similarity_score > 0.92 → HIGH CONFIDENCE same pattern
  → Skip full LLM reasoning, use cached RCA
  → Estimated time saving: 80% on repeat incidents
```

### Reasoning Agent
```
Aggregates all three agent outputs into a single structured prompt:

System: "You are an SRE expert. Analyze this production incident."

Context:
  Timeline: [ordered event chain]
  Service Graph: [affected subgraph + causal weights]
  Similar Incidents: [top-3 past matches with their root causes]
  Metrics: [Kafka lag spike time, token expiry time, delta]

Task: "Determine root cause, causal chain, and recommended fix.
       Output as JSON:
       {
         root_cause: { service, event_type, timestamp },
         causal_chain: [...],
         confidence: 0.0-1.0,
         recommendation: { immediate, short_term, long_term },
         similar_incident_reference: incident_id | null
       }"
```

---

## Data Flow for the Kafka + Token Case

```
T+0ms    auth-svc logs: TOKEN_EXPIRY
              │ event_id: evt-001
              │ vector_clock: {auth:5}
              ▼
         Vector Clock Injector assigns: VC={auth:5, order:0, pay:0}

T+50ms   Log Collector picks up → Normalizer → Embedding
         embed("auth token expired oauth2 service auth-svc")
         → vector: [0.23, -0.11, 0.87, ...]

T+60ms   Semantic Event Mapper → labels: [AUTH_FAILURE, TOKEN_LIFECYCLE]
         → Published to kafka: semantic-events, partition=auth-svc

T+312ms  order-consumer logs: KAFKA_CONSUMER_KICKOUT
              │ causation_id: evt-001  ← links back to token expiry
              │ vector_clock: {auth:5, order:3}
              ▼
         Dedup layer: groups evt-001 + evt-002 → CASCADE_INCIDENT

T+400ms  LangGraph triggered with grouped incident

T+401ms  [Timeline Agent]    → root=evt-001 (auth:5 < order:3 in VC)
T+402ms  [Graph Agent]       → Neo4j: auth-svc -[CAUSES]-> order-consumer
T+403ms  [Similarity Agent]  → Qdrant: similar incident from 3 weeks ago
                                        root_cause=TOKEN_EXPIRY, fix=proactive_refresh

T+500ms  [Reasoning Agent]   → GPT-4o/Claude synthesizes
T+600ms  RCA Generated:
         {
           root_cause: { service: "auth-svc", event: "TOKEN_EXPIRY", confidence: 0.94 },
           causal_chain: ["auth-svc → order-consumer → payment-consumer → ... (8 more)"],
           recommendation: {
             immediate: "Rotate token for auth-svc",
             short_term: "Implement proactive refresh at 75% TTL",
             long_term:  "Add token health check to consumer startup"
           }
         }
```

---

## Open Questions / Design Decisions Remaining

| Question | Options | Recommendation |
|---|---|---|
| Who assigns vector clocks? | Client-side SDK vs gateway vs injector sidecar | Gateway/injector — don't pollute service code |
| Neo4j update frequency? | Real-time vs batch | Batch every 5min — real-time Neo4j writes under load cause contention |
| LLM provider? | GPT-4o vs Claude vs self-hosted | Claude for structured JSON output; self-hosted fallback for air-gapped envs |
| Qdrant vs Pinecone? | Managed vs self-hosted | Qdrant self-hosted — keeps incident data inside your infra |
| Agent timeout? | How long before RCA must complete? | Hard limit 30s; cache similar-incident result for instant response on repeat patterns |
| Who triggers LangGraph? | Correlation Engine push vs cron poll | Push — lower latency, but add circuit breaker to prevent agent flood |

---

## Summary of Changes to Your Architecture

```
YOUR DESIGN              WHAT TO ADD                    WHY
───────────────────────────────────────────────────────────────────
Log Collector         +  Vector Clock Injector          Fix clock skew / causality
Normalizer            +  Dead Letter Queue              Handle malformed logs
Embedding Service     +  Embedding Cache (Redis)        Avoid redundant API calls
                      +  Local model fallback           Resilience if OpenAI token expires
(missing)             +  Alert Dedup Layer              1 RCA per cascade, not 10
Incident Query API    +  Incident Store (Postgres)      Give the API a real read model
RCA Generator         +  Learning Pipeline              System improves over time
                      +  Neo4j edge weight updates      Graph gets smarter each incident
(all agents)          +  Confidence Scorer              Know when to trust the RCA
```
