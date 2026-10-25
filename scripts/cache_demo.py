"""Prove the semantic cache end to end on a scratch store: miss, identical hit, paraphrase hit,
different-building miss, and the account-ID guard. Uses the real FastAPI app in-process.
    python scripts/cache_demo.py
"""
import os
import sys
import tempfile
import time

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='cache-demo-')
os.environ.setdefault('FASTEMBED_CACHE', os.path.abspath('data/fastembed'))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi.testclient import TestClient  # noqa: E402

from app.api import app  # noqa: E402

CASES = [('Wi-Fi keeps dropping in North Hall', False, 'first ask: miss, full swarm run'),
         ('Wi-Fi keeps dropping in North Hall', True, 'identical re-send: hit'),
         ('wifi keeps disconnecting in north hall', True, 'paraphrase: semantic hit'),
         ('Wi-Fi keeps dropping in East Hall', False, 'different building: miss'),
         ('My account STU-1001 is locked', False, 'miss'),
         ('My account STU-1002 is locked', False, 'near-identical text, other account ID: guarded miss')]

with TestClient(app) as client:
    stats = [client.get('/stats').json()['cache']]
    ok = True
    for question, expect_hit, label in CASES:
        start = time.perf_counter()
        r = client.post('/analyze', json={'question': question}).json()
        stats.append(client.get('/stats').json()['cache'])
        ok &= r['cached'] == expect_hit
        print(f"{(time.perf_counter() - start) * 1000:8.1f} ms  cached={r['cached']!s:5} "
              f"sim={r.get('similarity', '-')!s:6}  cost=${r['cost_usd']:.6f}  hits={stats[-1]['hits']}  "
              f"{question!r:44} {label}")
    final = stats[-1]
    print(f"\nfinal stats: {final}")
    print('CACHE DEMO: ' + ('PASS' if ok and final['hits'] == 2 else 'FAIL'))
    sys.exit(0 if ok and final['hits'] == 2 else 1)
