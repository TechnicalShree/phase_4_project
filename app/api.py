"""FastAPI gateway in front of the campus IT swarm."""
import queue
import threading
import time
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app import llm, store
from app.events import sse
from app.swarm import GRAPH

app = FastAPI(title='Campus IT Swarm Gateway')


class Ask(BaseModel):
    question: str = Field(min_length=3, max_length=2000)


def run_swarm(question, emit=lambda event: None):
    """Run the swarm once, forwarding agent/tool/token events to `emit`, and log the run."""
    run_id, start, state, error = uuid.uuid4().hex[:12], time.perf_counter(), {}, None
    emit({'type': 'run', 'run_id': run_id})
    try:
        for mode, chunk in GRAPH.stream({'question': question}, stream_mode=['custom', 'values']):
            if mode == 'custom':
                emit(chunk)
            else:
                state = chunk
    except Exception as exc:  # recorded as an error run; the monitor counts these
        error = f'{type(exc).__name__}: {exc}'
    latency_ms = round((time.perf_counter() - start) * 1000, 1)
    store.execute('INSERT INTO runs(id, question, answer, category, severity, status, error, latency_ms) '
                  'VALUES (?,?,?,?,?,?,?,?)', run_id, question, state.get('answer'), state.get('category'),
                  state.get('severity'), 'error' if error else 'ok', error, latency_ms)
    return {'run_id': run_id, 'status': 'error' if error else 'ok', 'error': error, 'latency_ms': latency_ms,
            'category': state.get('category'), 'severity': state.get('severity'), 'answer': state.get('answer')}


@app.get('/health')
def health():
    return {'status': 'ok', 'tiers': llm.tiers()}


@app.post('/analyze')
def analyze(body: Ask):
    result = run_swarm(body.question)
    if result['status'] == 'error':
        raise HTTPException(502, result)
    return result


@app.post('/analyze/stream')
def analyze_stream(body: Ask):
    """SSE: run, agent, tool_call and token events as they happen, then one final (or error) event.

    The swarm runs in its own thread, so a client disconnect never aborts a run half-way.
    """
    events = queue.Queue()

    def work():
        result = run_swarm(body.question, events.put)
        events.put({'type': 'error' if result['status'] == 'error' else 'final', 'result': result})
        events.put(None)

    threading.Thread(target=work, daemon=True).start()
    return StreamingResponse((sse(event) for event in iter(events.get, None)), media_type='text/event-stream')
