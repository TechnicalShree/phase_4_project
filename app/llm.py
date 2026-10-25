"""Model providers. A tier spec is 'kind:model' — an OpenAI-compatible endpoint, a scripted test double, or static."""
import os
import re
import time
from functools import lru_cache

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

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


def stream(agent, messages, offline):
    yield from stream_text(tiers()[0], messages, offline)


def complete(agent, messages, offline):
    return ''.join(stream(agent, messages, offline))
