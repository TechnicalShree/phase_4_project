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


class FinalEvent(BaseModel):
    type: Literal['final', 'error']
    result: dict


Event = TypeAdapter(Annotated[RunEvent | AgentEvent | ToolCallEvent | TokenEvent | FinalEvent,
                              Field(discriminator='type')])


def sse(event):
    event = Event.validate_python(event)
    return f'event: {event.type}\ndata: {event.model_dump_json()}\n\n'
