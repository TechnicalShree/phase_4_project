"""OpenTelemetry -> Arize Phoenix. OpenInference auto-instruments every LangGraph node and LangChain LLM call;
manual spans cover the run root and every model attempt (including the scripted/static tiers)."""
import os

from opentelemetry import trace

PHOENIX = os.getenv('PHOENIX_COLLECTOR_ENDPOINT', 'http://localhost:6006')
PROJECT = os.getenv('PHOENIX_PROJECT_NAME', 'campus-it-swarm')
tracer = trace.get_tracer('campus-it-swarm')


def setup():
    if os.getenv('TRACING', '1') == '0':
        return
    from openinference.instrumentation.langchain import LangChainInstrumentor
    from phoenix.otel import register
    provider = register(endpoint=f'{PHOENIX}/v1/traces', project_name=PROJECT, batch=True, verbose=False)
    LangChainInstrumentor().instrument(tracer_provider=provider)


def node_context():
    """OTel context of the running LangGraph node, so manual LLM spans nest under their agent."""
    try:
        from openinference.instrumentation.langchain import get_current_span
        span = get_current_span()
    except Exception:  # tracing disabled / instrumentor not active
        span = None
    return trace.set_span_in_context(span) if span else None


def ids(span):
    context = span.get_span_context()
    return format(context.trace_id, '032x'), format(context.span_id, '016x')
