from __future__ import annotations
import json
import os
import re
import shutil
from pathlib import Path

import chromadb
import numpy as np
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder

from src.delta_pipeline import get_spark, SILVER
from src.lineage import lineage_stage
from src.settings import CHROMA_ROOT, RAG_RESULT, PROJECT_ROOT

def _chunks(records: list[dict]) -> list[dict]:
    chunks = []
    for record in records:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", record["text"]) if s.strip()]
        for idx, sentence in enumerate(sentences or [record["text"]]):
            chunks.append({
                "id": f'{record["regulation_id"]}_chunk_{idx}',
                "text": sentence,
                "source": record["regulation_id"],
                "title": record["title"],
                "category": record["category"],
            })
    return chunks

@lineage_stage(
    "07_rag_pipeline",
    ["data/delta/silver"],
    ["data/chroma", "docs/rag_answer.json"],
)
def build_and_query_rag() -> dict:
    if os.getenv('FORCE_QUALITY_FAILURE') == '1':
        marker = PROJECT_ROOT / 'docs' / 'ERROR_rag_ran_after_failed_gate.txt'
        marker.write_text('RAG incorrectly ran after a failed quality gate', encoding='utf-8')
    spark = get_spark()
    records = [row.asDict() for row in spark.read.format("delta").load(str(SILVER)).select(
        "regulation_id", "title", "category", "text"
    ).collect()]
    spark.stop()

    chunks = _chunks(records)
    if CHROMA_ROOT.exists():
        shutil.rmtree(CHROMA_ROOT)
    CHROMA_ROOT.mkdir(parents=True, exist_ok=True)

    embedding_fn = SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    client = chromadb.PersistentClient(path=str(CHROMA_ROOT))
    collection = client.get_or_create_collection(
        name="university_regulations",
        embedding_function=embedding_fn,
    )
    collection.add(
        ids=[c["id"] for c in chunks],
        documents=[c["text"] for c in chunks],
        metadatas=[{"source":c["source"],"title":c["title"],"category":c["category"]} for c in chunks],
    )

    corpus = [re.findall(r"\w+", c["text"].lower()) for c in chunks]
    bm25 = BM25Okapi(corpus)
    reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

    query = "What happens when a student misses a final examination?"
    vector = collection.query(query_texts=[query], n_results=min(5, len(chunks)))
    vector_hits = [
        {"id":vector["ids"][0][i], "text":vector["documents"][0][i], "metadata":vector["metadatas"][0][i]}
        for i in range(len(vector["ids"][0]))
    ]

    scores = bm25.get_scores(re.findall(r"\w+", query.lower()))
    order = np.argsort(scores)[::-1][:5]
    keyword_hits = [chunks[i] for i in order]

    fused_scores, docs = {}, {}
    for rank, hit in enumerate(vector_hits, 1):
        fused_scores[hit["id"]] = fused_scores.get(hit["id"], 0) + 1/(60+rank)
        docs[hit["id"]] = hit
    for rank, hit in enumerate(keyword_hits, 1):
        fused_scores[hit["id"]] = fused_scores.get(hit["id"], 0) + 1/(60+rank)
        docs[hit["id"]] = {
            "id":hit["id"], "text":hit["text"],
            "metadata":{"source":hit["source"],"title":hit["title"],"category":hit["category"]}
        }

    candidates = [docs[key] for key in sorted(fused_scores, key=fused_scores.get, reverse=True)]
    rerank_scores = reranker.predict([[query, item["text"]] for item in candidates])
    ranked = sorted(
        [item | {"rerank_score":float(score)} for item, score in zip(candidates, rerank_scores)],
        key=lambda item:item["rerank_score"],
        reverse=True,
    )[:3]

    top = ranked[0]
    answer = {
        "question": query,
        "grounded_answer": top["text"],
        "citations": [
            {"source":item["metadata"]["source"],"title":item["metadata"]["title"],"text":item["text"]}
            for item in ranked
        ],
        "pipeline": ["chunking","embeddings","ChromaDB","BM25","RRF","cross-encoder reranking"],
    }
    RAG_RESULT.parent.mkdir(parents=True, exist_ok=True)
    RAG_RESULT.write_text(json.dumps(answer, indent=2), encoding="utf-8")
    print(json.dumps(answer, indent=2))
    return answer
