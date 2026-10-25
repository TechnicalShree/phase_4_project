"""End-to-end checks against the real app (demo tiers, scratch store, no tracing export)."""
import json
import os
import tempfile
import time

os.environ.update(DATA_DIR=tempfile.mkdtemp(prefix='pytest-'), TRACING='0', MODEL_MODE='demo', SCRIPTED_DELAY_S='0',
                  ENABLE_FAULT_INJECTION='1')
os.environ.setdefault('FASTEMBED_CACHE', os.path.abspath('data/fastembed'))

from fastapi.testclient import TestClient  # noqa: E402

from app import cascade  # noqa: E402
from app.api import app  # noqa: E402

client = TestClient(app)


def sse_events(question, **body):
    with client.stream('POST', '/analyze/stream', json={'question': question, **body}) as response:
        assert response.status_code == 200
        return [json.loads(line[6:]) for line in response.iter_lines() if line.startswith('data: ')]


def test_sync_analyze():
    result = client.post('/analyze', json={'question': 'Printer jams in the library', 'no_cache': True}).json()
    assert result['status'] == 'ok' and result['category'] == 'hardware' and result['answer']


def test_stream_event_order_and_tokens_match_answer():
    events = sse_events('VPN fails for STU-1001', no_cache=True)
    types = [e['type'] for e in events]
    assert types[0] == 'run' and types[-1] == 'final'
    assert [e['agent'] for e in events if e['type'] == 'agent'] == ['triage', 'researcher', 'specialist', 'writer']
    assert 'tool_call' in types
    assert ''.join(e['text'] for e in events if e['type'] == 'token').strip() == events[-1]['result']['answer']


def test_async_job_completes():
    accepted = client.post('/analyze/async', json={'question': 'My password expired', 'no_cache': True})
    assert accepted.status_code == 202
    for _ in range(50):
        job = client.get(accepted.json()['poll']).json()
        if job['status'] in ('done', 'failed'):
            break
        time.sleep(0.1)
    assert job['status'] == 'done' and job['result']['answer']


def test_identical_query_hits_cache():
    question = 'Swollen laptop battery in East Hall'
    first = client.post('/analyze', json={'question': question}).json()
    second = client.post('/analyze', json={'question': question}).json()
    assert not first['cached'] and second['cached'] and second['answer'] == first['answer']


def test_mid_stream_hot_swap_keeps_one_stream():
    cascade.reset()
    client.post('/admin/fault', json={'position': 0, 'after_chunks': 4})
    events = sse_events('Wi-Fi outage in North Hall', no_cache=True)
    tiers = [e['tier'] for e in events if e['type'] == 'token']
    assert tiers[:4] == [0, 0, 0, 0] and set(tiers[4:]) == {1}
    assert any(e['type'] == 'fallback' for e in events) and events[-1]['type'] == 'final'


def test_cascade_standalone_self_check():
    cascade._demo()
    cascade.reset()
