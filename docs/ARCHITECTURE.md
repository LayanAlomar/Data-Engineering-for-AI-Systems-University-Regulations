# Architecture

```mermaid
flowchart LR
    A[JSON source events] --> B[Real Kafka producer]
    B --> C[(Kafka raw topic)]
    C --> D[Real Kafka consumer + Pydantic contract]
    D -->|valid| E[(Kafka valid topic)]
    D -->|malformed + reason| F[(Kafka DLQ topic)]
    D --> G[Landing JSONL]
    G --> H[Delta Bronze + MERGE + schema proof]
    H --> I[Delta Silver]
    I --> J[Great Expectations quality gate]
    J -->|success only| K[Delta Gold aggregate]
    J -->|success only| L[Hybrid RAG]
    I --> M[Chroma embeddings]
    I --> N[BM25]
    M --> O[RRF fusion]
    N --> O
    O --> P[Cross-encoder reranking]
    P --> Q[Grounded answer + citations]
    R[Airflow DAG] --> B
    R --> D
    R --> H
    R --> I
    R --> J
    R --> K
    R --> L
    S[OpenLineage] -. START / COMPLETE / FAIL per stage .-> R
```

Airflow dependency chain:

`kafka_produce → kafka_consume_validate_dlq → delta_bronze_merge_schema_enforcement → delta_silver → great_expectations_quality_gate → [delta_gold_aggregate, hybrid_rag_with_reranking]`

Gold and RAG are both downstream of the quality gate. A failed Great Expectations task prevents both from running.
