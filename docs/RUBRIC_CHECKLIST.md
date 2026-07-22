# Final rubric checklist

## Ingestion — 20 points

- [x] Real Apache Kafka broker
- [x] Real Kafka producer
- [x] Real Kafka consumer
- [x] Pydantic validation at ingestion boundary
- [x] Real Kafka DLQ
- [x] Rejection reason retained
- [x] Malformed records demonstrated

## Delta Lakehouse — 25 points

- [x] Real Delta Bronze
- [x] Real Delta Silver
- [x] Real aggregate Gold
- [x] Real MERGE keyed on `regulation_id`
- [x] Schema enforcement rejection demonstrated

## RAG — 25 points

- [x] Document chunking
- [x] Sentence-transformer embeddings
- [x] Chroma vector store
- [x] BM25 keyword retrieval
- [x] Reciprocal Rank Fusion
- [x] Cross-encoder reranking
- [x] Grounded answer with citations

## Orchestration — 15 points

- [x] Real Airflow DAG
- [x] Correct stage dependencies
- [x] Failed quality gate blocks downstream RAG
- [x] Successful and failed DAG logs retained

## Quality and lineage — 15 points

- [x] Great Expectations checks
- [x] Quality checks are an actual Airflow gate
- [x] OpenLineage START events
- [x] OpenLineage COMPLETE events
- [x] OpenLineage FAIL events
- [x] Events emitted per executed stage

## GitHub and documentation

- [x] Professional README
- [x] Architecture documentation
- [x] Sensible repository structure
- [x] `.gitignore`
- [x] Training program attribution
- [x] SDAIA Academy GitHub link
- [ ] Publish to trainee's GitHub account
- [ ] Create authentic incremental commit history
