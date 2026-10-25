"""Typed SSE event schemas. The SSE `event:` field carries `type`, so clients can switch on it directly."""
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter


class RunEvent(BaseModel):
    type: Literal['run']
    run_id: str


class AgentEvent(BaseModel):
    type: Literal['agent']
    agent: str


class ToolCallEvent(BaseModel):
    type: Literal['tool_call']
    tool: str
    args: dict
    result: Any


class TokenEvent(BaseModel):
    type: Literal['token']
    text: str
    tier: int
    model: str


class CascadeEvent(BaseModel):
    """fallback: tier `position` failed (after `emitted_chars`) and `next_position` takes over the same stream.
    circuit_open: that failure tripped the tier's breaker. circuit_skip: an open breaker skipped the tier."""
    type: Literal['fallback', 'circuit_open', 'circuit_skip']
    agent: str
    run_id: str | None
    position: int
    model: str
    state: str | None = None
    error: str | None = None
    emitted_chars: int | None = None
    next_position: int | None = None


class CacheHitEvent(BaseModel):
    type: Literal['cache_hit']
    similarity: float
    cached_question: str
    saved_usd: float | None


class FinalEvent(BaseModel):
    type: Literal['final', 'error']
    result: dict


Event = TypeAdapter(Annotated[RunEvent | AgentEvent | ToolCallEvent | TokenEvent | CascadeEvent | CacheHitEvent | FinalEvent,
                              Field(discriminator='type')])


def sse(event):
    event = Event.validate_python(event)
    return f'event: {event.type}\ndata: {event.model_dump_json()}\n\n'
