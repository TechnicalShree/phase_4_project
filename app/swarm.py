"""Campus IT support swarm: triage -> researcher -> specialist -> writer."""
import json
import re
import sys
from typing import TypedDict

from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph

from app import llm
from app.domain import (ACCOUNT_ID, CATEGORIES, CRITICAL, check_service_status, keyword_triage,
                        lookup_campus_account, search_knowledge_base)


class State(TypedDict, total=False):
    question: str
    category: str
    severity: str
    tool_results: list[dict]
    diagnosis: str
    answer: str


def announce(agent):
    get_stream_writer()({'type': 'agent', 'agent': agent})


def triage(state):
    announce('triage')
    offline = keyword_triage(state['question'])
    raw = llm.complete('triage', [
        ('system', 'Classify a campus IT request. Reply with JSON only: {"category": one of '
         'account|network|hardware|general, "severity": "high" for outages, security compromise, account '
         'deletion or unsafe hardware, otherwise "low"}.'),
        ('human', state['question'])], json.dumps(offline))
    try:
        result = json.loads(re.search(r'\{.*\}', raw, re.S)[0])
        result = {'category': result['category'], 'severity': result['severity']}
        assert result['category'] in CATEGORIES and result['severity'] in ('low', 'high')
    except (TypeError, ValueError, KeyError, AssertionError):
        result = offline
    if any(word in state['question'].lower() for word in CRITICAL):
        result['severity'] = 'high'  # deterministic floor; the model may not downgrade a critical report
    return result


def researcher(state):
    announce('researcher')
    calls = [('search_knowledge_base', {'category': state['category']}, search_knowledge_base)]
    if account := ACCOUNT_ID.search(state['question']):
        calls.append(('lookup_campus_account', {'account_id': account[0]}, lookup_campus_account))
    if state['category'] == 'network':
        calls.append(('check_service_status', {'service': 'wifi'}, check_service_status))
    results = []
    for name, args, fn in calls:
        result = fn(*args.values())
        get_stream_writer()({'type': 'tool_call', 'tool': name, 'args': args, 'result': result})
        results.append({'tool': name, 'args': args, 'result': result})
    return {'tool_results': results}


def facts(state):
    return '\n'.join(f"- {r['tool']}: {json.dumps(r['result'])}" for r in state['tool_results'])


def specialist(state):
    announce('specialist')
    offline = ' '.join(str(r['result']) for r in state['tool_results'] if r['tool'] == 'search_knowledge_base')
    diagnosis = llm.complete('specialist', [
        ('system', f"You are the campus {state['category']} specialist. In at most 3 sentences, diagnose the "
         'likely cause using ONLY these facts. Never claim to have changed an account.\n' + facts(state)),
        ('human', state['question'])], offline)
    return {'diagnosis': diagnosis.strip()}


def offline_answer(state):
    lines = [f"Campus IT ({state['category']}, {state['severity']} severity):", state['diagnosis']]
    for r in state['tool_results']:
        if r['tool'] == 'check_service_status':
            lines.append(f"Service status: {r['result']['service']} is {r['result']['status']}.")
        if r['tool'] == 'lookup_campus_account':
            lines.append('Account record: ' + ', '.join(f'{k} {v}' for k, v in r['result'].items()) + '.')
    if state['severity'] == 'high':
        lines.append('This is high severity: the service desk should open a ticket now.')
    return '\n'.join(lines)


def writer(state):
    announce('writer')
    emit = get_stream_writer()
    answer = ''
    for chunk in llm.stream('writer', [
        ('system', 'You are campus IT support writing the final reply. Give a short diagnosis and numbered '
         'next steps (max 120 words) using ONLY the facts and diagnosis below. Never claim to have changed, '
         'reset or deleted an account. If severity is high, say a ticket should be opened.\n'
         f"Category: {state['category']}, severity: {state['severity']}\nFacts:\n{facts(state)}\n"
         f"Diagnosis: {state['diagnosis']}"),
        ('human', state['question'])], offline_answer(state)):
        answer += chunk
        emit({'type': 'token', 'text': chunk})
    return {'answer': answer.strip()}


def build_graph():
    graph = StateGraph(State)
    for name, node in (('triage', triage), ('researcher', researcher),
                       ('specialist', specialist), ('writer', writer)):
        graph.add_node(name, node)
    graph.add_edge(START, 'triage')
    graph.add_edge('triage', 'researcher')
    graph.add_edge('researcher', 'specialist')
    graph.add_edge('specialist', 'writer')
    graph.add_edge('writer', END)
    return graph.compile()


GRAPH = build_graph()

if __name__ == '__main__':
    question = ' '.join(sys.argv[1:]) or 'Wi-Fi keeps dropping in North Hall for STU-1001'
    for mode, event in GRAPH.stream({'question': question}, stream_mode=['custom', 'values']):
        if mode == 'custom' and event['type'] == 'token':
            print(event['text'], end='', flush=True)
        elif mode == 'custom':
            print(f'\n[{event["type"]}] {event.get("agent") or event.get("tool")}', flush=True)
    print('\n\nfinal state:', json.dumps({k: v for k, v in event.items() if k != 'tool_results'}, indent=1))
