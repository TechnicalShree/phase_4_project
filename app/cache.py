"""Semantic cache in front of the swarm, with a cost model (hit rate -> dollars and seconds saved).

Embeddings: BAAI/bge-small-en-v1.5 via fastembed (local ONNX, no API). A hit needs cosine >= CACHE_THRESHOLD,
an entry younger than CACHE_TTL_S, and the exact same campus account IDs (so STU-1001 never gets STU-1002's answer).
"""
import json
import os
import time
from functools import lru_cache

import numpy as np

from app import store
from app.domain import ACCOUNT_ID

THRESHOLD = float(os.getenv('CACHE_THRESHOLD', '0.92'))
TTL_S = float(os.getenv('CACHE_TTL_S', '3600'))


@lru_cache
def model():
    from fastembed import TextEmbedding
    return TextEmbedding('BAAI/bge-small-en-v1.5', cache_dir=os.getenv('FASTEMBED_CACHE', 'data/fastembed'))


def embed(text):
    vector = next(iter(model().embed([text.strip().lower()]))).astype(np.float32)
    return vector / np.linalg.norm(vector)


def account_ids(text):
    return json.dumps(sorted({match.upper() for match in ACCOUNT_ID.findall(text)}))


def lookup(question):
    vector = embed(question)
    rows = store.query('SELECT * FROM cache WHERE ids=? AND created_at>?', account_ids(question), time.time() - TTL_S)
    if rows:
        # ponytail: linear scan over the matrix; swap in an ANN index (sqlite-vec/FAISS) past ~50k entries.
        sims = np.stack([np.frombuffer(r['embedding'], dtype=np.float32) for r in rows]) @ vector
        best = int(np.argmax(sims))
        if sims[best] >= THRESHOLD:
            row = rows[best]
            store.execute("INSERT INTO events(kind, name, detail) VALUES ('cache', 'hit', ?)", json.dumps({
                'similarity': float(sims[best]), 'saved_usd': row['cost_usd'], 'saved_ms': row['latency_ms']}))
            return {'similarity': round(float(sims[best]), 4), 'cached_question': row['question'],
                    'saved_usd': row['cost_usd'], 'result': json.loads(row['result'])}
    store.execute("INSERT INTO events(kind, name, detail) VALUES ('cache', 'miss', '{}')")
    return None


def put(question, result, cost_usd, latency_ms):
    store.execute('INSERT INTO cache(question, ids, embedding, result, cost_usd, latency_ms) VALUES (?,?,?,?,?,?)',
                  question, account_ids(question), embed(question).tobytes(), json.dumps(result), cost_usd, latency_ms)


def stats():
    rows = store.query("SELECT name, detail FROM events WHERE kind='cache'")
    hits = [json.loads(r['detail']) for r in rows if r['name'] == 'hit']
    spend = store.query('SELECT COALESCE(SUM(cost_usd), 0) AS usd FROM llm_calls')[0]['usd']
    saved = sum(h['saved_usd'] or 0 for h in hits)
    return {'lookups': len(rows), 'hits': len(hits), 'hit_rate': round(len(hits) / len(rows), 4) if rows else 0.0,
            'entries': store.query('SELECT COUNT(*) AS n FROM cache')[0]['n'],
            'saved_usd': round(saved, 6), 'spent_usd': round(spend, 6),
            'saved_pct_of_would_be_spend': round(saved / (saved + spend), 4) if saved + spend else 0.0,
            'saved_seconds': round(sum(h['saved_ms'] or 0 for h in hits) / 1000, 2)}
