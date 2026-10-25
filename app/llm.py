"""Model providers. A tier spec is 'kind:model' — an OpenAI-compatible endpoint, a scripted test double, or static."""
import os
import re
import time
from functools import lru_cache

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.config import get_config

from app import store
from app.tracing import node_context, tracer

load_dotenv()
DEMO_TIERS = ['scripted:demo-frontier', 'scripted:demo-utility', 'static']
ENDPOINTS = {  # kind -> (base_url, api-key env var)
    'openrouter': ('https://openrouter.ai/api/v1', 'OPENROUTER_API_KEY'),
    'openai': (os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1'), 'OPENAI_API_KEY'),
    'ollama': (os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434/v1'), None),
}


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


def record(agent, position, spec, messages, text):
    # ponytail: ~4 chars/token estimate; read provider usage_metadata if billing-grade accuracy is needed.
    tokens = (sum(len(str(m[1])) for m in messages) + len(text)) // 4
    cost = tokens * price(position) / 1e6
    store.execute('INSERT INTO llm_calls(run_id, agent, position, model, tokens, cost_usd) VALUES (?,?,?,?,?,?)',
                  current_run_id(), agent, position, spec, tokens, cost)


def stream(agent, messages, offline):
    position = 0
    spec = tiers()[position]
    with tracer.start_as_current_span(f'llm.{agent}', context=node_context(), attributes={
            'openinference.span.kind': 'LLM', 'llm.model_name': spec, 'agent': agent,
            'input.value': str(messages[-1][1])}) as span:
        text = ''
        for chunk in stream_text(spec, messages, offline):
            text += chunk
            yield chunk
        span.set_attribute('output.value', text)
        record(agent, position, spec, messages, text)


def complete(agent, messages, offline):
    return ''.join(stream(agent, messages, offline))
