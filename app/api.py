"""FastAPI gateway in front of the campus IT swarm."""
import json
import queue
import threading
import time
import uuid

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel, Field

from app import cache, llm, store, tracing
from app.events import sse
from app.swarm import GRAPH
from app.tracing import ids, tracer

tracing.setup()
app = FastAPI(title='Campus IT Swarm Gateway')


class Ask(BaseModel):
    question: str = Field(min_length=3, max_length=2000)
    no_cache: bool = False


def run_swarm(question, emit=lambda event: None, no_cache=False):
    """Run the swarm once (or serve it from the semantic cache), forwarding events to `emit`, and log the run."""
    run_id, start, state, error = uuid.uuid4().hex[:12], time.perf_counter(), {}, None
    emit({'type': 'run', 'run_id': run_id})
    if not no_cache and (hit := cache.lookup(question)):
        with tracer.start_as_current_span('campus_swarm.cache_hit', attributes={
                'openinference.span.kind': 'CHAIN', 'input.value': question, 'run.id': run_id,
                'cache.similarity': hit['similarity'], 'output.value': hit['result']['answer']}):
            pass
        emit({'type': 'cache_hit', **{k: hit[k] for k in ('similarity', 'cached_question', 'saved_usd')}})
        return {**hit['result'], 'run_id': run_id, 'cached': True, 'similarity': hit['similarity'],
                'served_from_run': hit['result']['run_id'], 'cost_usd': 0.0,
                'latency_ms': round((time.perf_counter() - start) * 1000, 1)}
    with tracer.start_as_current_span('campus_swarm.run', attributes={
            'openinference.span.kind': 'CHAIN', 'input.value': question, 'run.id': run_id}) as span:
        trace_id, span_id = ids(span)
        try:
            for mode, chunk in GRAPH.stream({'question': question}, {'configurable': {'run_id': run_id}},
                                            stream_mode=['custom', 'values']):
                if mode == 'custom':
                    emit(chunk)
                else:
                    state = chunk
        except Exception as exc:  # recorded as an error run; the monitor counts these
            error = f'{type(exc).__name__}: {exc}'
            span.set_status(Status(StatusCode.ERROR, error))
        span.set_attribute('output.value', state.get('answer') or '')
    latency_ms = round((time.perf_counter() - start) * 1000, 1)
    cost_usd = store.query('SELECT COALESCE(SUM(cost_usd), 0) AS usd FROM llm_calls WHERE run_id=?', run_id)[0]['usd']
    store.execute('INSERT INTO runs(id, trace_id, span_id, question, answer, category, severity, tool_results, '
                  'status, error, latency_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?)', run_id, trace_id, span_id, question,
                  state.get('answer'), state.get('category'), state.get('severity'),
                  json.dumps(state.get('tool_results', [])), 'error' if error else 'ok', error, latency_ms)
    result = {'run_id': run_id, 'trace_id': trace_id, 'status': 'error' if error else 'ok', 'error': error,
              'latency_ms': latency_ms, 'cost_usd': round(cost_usd, 8), 'cached': False,
              'category': state.get('category'), 'severity': state.get('severity'), 'answer': state.get('answer')}
    if not error and not no_cache:
        cache.put(question, result, cost_usd, latency_ms)
    return result


@app.get('/health')
def health():
    return {'status': 'ok', 'tiers': llm.tiers()}


@app.post('/analyze')
def analyze(body: Ask):
    result = run_swarm(body.question, no_cache=body.no_cache)
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
        result = run_swarm(body.question, events.put, body.no_cache)
        events.put({'type': 'error' if result['status'] == 'error' else 'final', 'result': result})
        events.put(None)

    threading.Thread(target=work, daemon=True).start()
    return StreamingResponse((sse(event) for event in iter(events.get, None)), media_type='text/event-stream')


def run_job(job_id, question, no_cache=False):
    store.execute("UPDATE jobs SET status='running', updated_at=unixepoch('subsec') WHERE id=?", job_id)
    result = run_swarm(question, no_cache=no_cache)
    store.execute("UPDATE jobs SET status=?, result=?, updated_at=unixepoch('subsec') WHERE id=?",
                  'failed' if result['status'] == 'error' else 'done', json.dumps(result), job_id)


@app.post('/analyze/async', status_code=202)
def analyze_async(body: Ask, background: BackgroundTasks):
    # ponytail: in-process background task on one worker; move to a Redis/arq queue for multi-worker deploys.
    job_id = uuid.uuid4().hex[:12]
    store.execute("INSERT INTO jobs(id, question, status) VALUES (?, ?, 'queued')", job_id, body.question)
    background.add_task(run_job, job_id, body.question, body.no_cache)
    return {'job_id': job_id, 'status': 'queued', 'poll': f'/jobs/{job_id}'}


@app.get('/jobs/{job_id}')
def get_job(job_id: str):
    rows = store.query('SELECT * FROM jobs WHERE id=?', job_id)
    if not rows:
        raise HTTPException(404, 'unknown job')
    job = rows[0]
    job['result'] = json.loads(job['result']) if job['result'] else None
    return job


@app.on_event('startup')
def fail_orphaned_jobs():
    """Jobs live in-process: any job still queued/running at boot died with the previous process."""
    store.execute("UPDATE jobs SET status='failed', result='{\"error\": \"server restarted\"}' "
                  "WHERE status IN ('queued', 'running')")


@app.get('/stats')
def stats():
    return {'cache': cache.stats(), **llm.budget_report()}


@app.on_event('startup')
def warm_embeddings():
    cache.embed('warm up')  # load the ONNX model once at boot, not on the first request
