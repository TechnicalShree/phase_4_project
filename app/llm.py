"""Model providers. A tier spec is 'kind:model' — an OpenAI-compatible endpoint, a scripted test double, or static."""
import json
import os
import re
import time
from functools import lru_cache

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.config import get_config, get_stream_writer
from opentelemetry import trace

from app import cascade, store
from app.tracing import node_context, tracer

load_dotenv()
DEMO_TIERS = ['scripted:demo-frontier', 'scripted:demo-utility', 'static']
ENDPOINTS = {  # kind -> (base_url, api-key env var)
    'openrouter': ('https://openrouter.ai/api/v1', 'OPENROUTER_API_KEY'),
    'openai': (os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1'), 'OPENAI_API_KEY'),
    'ollama': (os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434/v1'), None),
}
# Router: each agent has a cost tier; a cost tier is a starting position in the tier list.
ROUTES = {'triage': 'utility', 'specialist': 'utility', 'writer': 'frontier', 'judge': 'frontier'}
START_POSITION = {'frontier': 0, 'utility': 1}
BUDGETS = {'frontier': float(os.getenv('BUDGET_FRONTIER_USD', '1.00')),
           'utility': float(os.getenv('BUDGET_UTILITY_USD', '0.25'))}


def tiers():
    if os.getenv('MODEL_MODE', 'demo') == 'demo':
        return DEMO_TIERS
    return [os.getenv('TIER_0', 'openrouter:openai/gpt-4.1-mini'),
            os.getenv('TIER_1', 'ollama:qcwind/qwen3-8b-instruct-Q4-K-M:latest'), 'static']


@lru_cache
def chat_model(spec):
    kind, _, name = spec.partition(':')
    base_url, key_env = ENDPOINTS[kind]
    key = os.getenv(key_env) if key_env else 'ollama'
    if not key:
        raise RuntimeError(f'{spec} needs {key_env} in .env')
    return ChatOpenAI(model=name, base_url=base_url, api_key=key, timeout=60, max_retries=0,
                      reasoning_effort='none' if kind == 'ollama' else None)


def stream_text(spec, messages, offline):
    """Yield text chunks from one tier. `offline` is the deterministic answer used by scripted/static tiers."""
    kind = spec.partition(':')[0]
    if kind == 'static':
        yield offline
    elif kind == 'scripted':
        # ponytail: scripted test double so the stack runs without keys; it is not an LLM.
        for word in re.findall(r'\S+\s*', offline):
            time.sleep(float(os.getenv('SCRIPTED_DELAY_S', '0.02')))
            yield word
    else:
        for chunk in chat_model(spec).stream(messages):
            if chunk.content:
                yield chunk.content


def price(position):
    """Notional USD per 1M tokens by tier position. Local Ollama and the scripted doubles have no invoice;
    they are priced as the hosted tier they stand in for, so the cost model has something to save."""
    return [float(os.getenv('TIER_0_USD_PER_MTOK', '2.0')), float(os.getenv('TIER_1_USD_PER_MTOK', '0.2')), 0.0][position]


def current_run_id():
    try:
        return get_config()['configurable'].get('run_id')
    except RuntimeError:  # called outside the graph (e.g. the judge)
        return None


def record(agent, cost_tier, position, spec, messages, text):
    # ponytail: ~4 chars/token estimate; read provider usage_metadata if billing-grade accuracy is needed.
    tokens = (sum(len(str(m[1])) for m in messages) + len(text)) // 4
    cost = tokens * price(position) / 1e6
    store.execute('INSERT INTO llm_calls(run_id, agent, cost_tier, position, model, tokens, cost_usd) '
                  'VALUES (?,?,?,?,?,?,?)', current_run_id(), agent, cost_tier, position, spec, tokens, cost)


def spent(cost_tier, window_s=86400):
    return store.query('SELECT COALESCE(SUM(cost_usd), 0) AS usd FROM llm_calls WHERE cost_tier=? AND created_at>?',
                       cost_tier, time.time() - window_s)[0]['usd']


def route(agent):
    """Agent -> (cost tier, reason). ROUTE_<AGENT>=frontier|utility overrides; an exhausted frontier budget
    downgrades to utility rather than overspending."""
    override = os.getenv(f'ROUTE_{agent.upper()}')
    cost_tier, reason = (override, 'override') if override in START_POSITION else (ROUTES.get(agent, 'utility'),
                                                                                  'default')
    if cost_tier == 'frontier' and spent('frontier') >= BUDGETS['frontier']:
        return 'utility', 'frontier budget exhausted'
    return cost_tier, reason


def budget_report():
    return {'routes': {agent: route(agent)[0] for agent in ROUTES},
            'budget_usd_24h': {t: {'budget': b, 'spent': round(spent(t), 6), 'remaining': round(b - spent(t), 6)}
                               for t, b in BUDGETS.items()}}


def emit(event):
    try:
        get_stream_writer()(event)
    except Exception:  # outside a graph run (judge, scripts): nothing to stream to
        pass


def attempt(agent, cost_tier, position, spec, parent):
    """One tier as a cascade provider: its own LLM span, cost recorded even for a partial (failed) attempt."""
    def provider(messages, offline):
        text = ''
        with tracer.start_as_current_span(f'llm.{agent}.tier{position}', context=parent, attributes={
                'openinference.span.kind': 'LLM', 'llm.model_name': spec, 'tier.position': position,
                'input.value': str(messages[-1][1])}) as span:
            try:
                for chunk in stream_text(spec, messages, offline):
                    text += chunk
                    yield chunk
            finally:
                span.set_attribute('output.value', text)
                record(agent, cost_tier, position, spec, messages, text)
    return provider


def stream(agent, messages, offline, resume=True):
    """Yield (position, model, chunk): start at the agent's routed cost tier, fall back down the cascade."""
    cost_tier, reason = route(agent)
    specs = tiers()
    with tracer.start_as_current_span(f'llm.{agent}', context=node_context(), attributes={
            'openinference.span.kind': 'CHAIN', 'agent': agent, 'route.cost_tier': cost_tier,
            'route.reason': reason, 'input.value': str(messages[-1][1])}) as span:
        parent = trace.set_span_in_context(span)
        providers = [attempt(agent, cost_tier, p, spec, parent) for p, spec in enumerate(specs)]

        def on_event(kind, **info):
            info = {'agent': agent, 'run_id': current_run_id(), 'model': specs[info['position']], **info}
            span.add_event(kind, {k: str(v) for k, v in info.items()})
            store.execute("INSERT INTO events(kind, name, detail) VALUES ('cascade', ?, ?)", kind, json.dumps(info))
            emit({'type': kind, **info})

        served = []
        for position, chunk in cascade.stream(providers, messages, offline, START_POSITION[cost_tier], resume,
                                              on_event):
            if position not in served:
                served.append(position)
            yield position, specs[position], chunk
        span.set_attribute('cascade.served_by', json.dumps(served))


def complete(agent, messages, offline):
    """Non-streamed call (structured output): a failed tier's partial text is discarded, the next restarts."""
    parts = list(stream(agent, messages, offline, resume=False))
    return ''.join(chunk for position, _, chunk in parts if position == parts[-1][0]) if parts else ''
