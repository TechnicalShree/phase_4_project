"""CUT CABLE — the failure drill. Self-contained: boots the real gateway on a real TCP port (scratch data dir),
then deliberately fails the primary tier and proves the system survives it.

  A. hot-swap   primary dies mid-answer; ONE SSE connection carries tier-0 then tier-1 chunks to a clean final
  B. breaker    two more primary failures trip the circuit; the next request skips tier 0 without touching it
  C. recovery   after the cooldown a half-open trial goes back to tier 0 and the circuit closes
  D. monitor    the daemon's cascade tripwires (fallback_rate, circuit_open_events) fire and page

    python scripts/cut_cable.py [--repeat N]     (MODEL_MODE from .env; demo needs no keys)
"""
import json
import os
import socket
import sys
import tempfile
import threading
import time

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='cut-cable-')
os.environ['ENABLE_FAULT_INJECTION'] = '1'
os.environ['BREAKER_THRESHOLD'] = '3'
# The cooldown must outlast one full request, or the circuit half-opens before phase B can observe it open.
os.environ['BREAKER_COOLDOWN_S'] = os.getenv('DRILL_COOLDOWN_S', '3' if os.getenv('MODEL_MODE', 'demo') == 'demo'
                                             else '60')
os.environ['PAGE_COOLDOWN_S'] = '0'  # page on every breach so each repeat proves the tripwire again
os.environ.setdefault('FASTEMBED_CACHE', os.path.abspath('data/fastembed'))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import httpx  # noqa: E402
import uvicorn  # noqa: E402

from app import monitor, store  # noqa: E402
from app.api import app  # noqa: E402

QUESTION = 'Wi-Fi outage in North Hall, STU-1001 cannot get online'
CUT_AFTER = 6
DIM, RED, GREEN, CYAN, YELLOW, RESET = (('\033[2m', '\033[31m', '\033[32m', '\033[36m', '\033[33m', '\033[0m')
                                        if sys.stdout.isatty() else ('',) * 6)
COLORS = {0: CYAN, 1: YELLOW, 2: GREEN}


def serve():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='warning'))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)
    return f'http://127.0.0.1:{port}', server


def stream_once(base, show=False):
    """One POST /analyze/stream over one TCP connection; returns (status, events, connections_used)."""
    events, transcript_tier = [], None
    with httpx.Client(base_url=base, timeout=180) as client:
        with client.stream('POST', '/analyze/stream', json={'question': QUESTION, 'no_cache': True}) as response:
            for line in response.iter_lines():
                if not line.startswith('data: '):
                    continue
                event = json.loads(line[6:])
                events.append(event)
                if not show:
                    continue
                if event['type'] == 'token':
                    if event['tier'] != transcript_tier:
                        transcript_tier = event['tier']
                        print(f"\n{COLORS[event['tier']]}[tier {event['tier']} · {event['model']}]{RESET} ", end='')
                    print(f"{COLORS[event['tier']]}{event['text']}{RESET}", end='', flush=True)
                elif event['type'] in ('fallback', 'circuit_open', 'circuit_skip'):
                    print(f"\n{RED}<< {event['type']} agent={event['agent']} tier={event['position']} "
                          f"{event.get('error') or event.get('state')} >>{RESET}", end='')
                elif event['type'] in ('final', 'error'):
                    print(f"\n{DIM}[{event['type']} event received, stream closed by server]{RESET}")
        # httpx pools connections: one request on a fresh client = one TCP connection, never re-opened
        connections = len(client._transport._pool.connections) if hasattr(client._transport, '_pool') else 1
    return response.status_code, events, connections


def tokens(events):
    return [e for e in events if e['type'] == 'token']


def kinds(events):
    return [e['type'] for e in events]


def drill(base, round_no):
    checks = {}
    httpx.delete(f'{base}/admin/fault')

    print(f'\n{"=" * 78}\nROUND {round_no} · A. HOT-SWAP — primary tier cut after {CUT_AFTER} chunks of the answer')
    httpx.post(f'{base}/admin/fault', json={'position': 0, 'after_chunks': CUT_AFTER, 'count': 1})
    status, events, connections = stream_once(base, show=True)
    served = [t['tier'] for t in tokens(events)]
    final = events[-1]
    checks['A: HTTP 200 on a single connection'] = status == 200 and connections == 1
    checks['A: first chunks from tier 0, rest from tier 1'] = (
        served[:CUT_AFTER] == [0] * CUT_AFTER and len(served) > CUT_AFTER and set(served[CUT_AFTER:]) == {1})
    checks['A: fallback event announced in-stream'] = any(
        e['type'] == 'fallback' and e['position'] == 0 and e['agent'] == 'writer' for e in events)
    checks['A: stream ended with a final event (no drop)'] = final['type'] == 'final' and final['result']['status'] == 'ok'
    checks['A: answer = tier-0 chunks + tier-1 chunks, nothing lost'] = (
        ''.join(t['text'] for t in tokens(events)).strip() == final['result']['answer'])

    print(f'\nROUND {round_no} · B. CIRCUIT BREAKER — primary fails outright twice more (threshold 3)')
    httpx.post(f'{base}/admin/fault', json={'position': 0, 'after_chunks': 0, 'count': 2})
    trips = [stream_once(base)[1] for _ in range(2)]
    checks['B: circuit opened on the 3rd consecutive failure'] = 'circuit_open' in kinds(trips[1])
    checks['B: /stats shows tier 0 breaker open'] = httpx.get(f'{base}/stats').json()['breakers']['0']['state'] == 'open'
    status, skipped, _ = stream_once(base, show=True)
    run_id = skipped[0]['run_id']
    attempts = store.query('SELECT DISTINCT position FROM llm_calls WHERE run_id=? AND agent=?', run_id, 'writer')
    checks['B: open circuit skips tier 0 (circuit_skip, no tier-0 call)'] = (
        'circuit_skip' in kinds(skipped) and [a['position'] for a in attempts] == [1]
        and {t['tier'] for t in tokens(skipped)} == {1} and skipped[-1]['type'] == 'final')

    cooldown = float(os.environ['BREAKER_COOLDOWN_S'])
    print(f'\nROUND {round_no} · C. RECOVERY — wait out the {cooldown:g}s cooldown, half-open trial on tier 0')
    deadline = time.time() + cooldown + 5
    while httpx.get(f'{base}/stats').json()['breakers']['0']['state'] == 'open' and time.time() < deadline:
        time.sleep(0.25)
    status, recovered, _ = stream_once(base)
    checks['C: half-open trial served by tier 0, circuit closed'] = (
        {t['tier'] for t in tokens(recovered)} == {0}
        and httpx.get(f'{base}/stats').json()['breakers']['0']['state'] == 'closed')
    print(f"   served by tiers {sorted({t['tier'] for t in tokens(recovered)})}, "
          f"breaker {httpx.get(f'{base}/stats').json()['breakers']['0']['state']}")

    print(f'\nROUND {round_no} · D. MONITOR — one daemon tick over the drill traffic')
    report = monitor.tick()
    breached = monitor.show(report)
    paged = {row['slo'] for row in report if row['paged']}
    checks['D: fallback_rate tripwire fired and paged'] = 'fallback_rate' in breached and 'fallback_rate' in paged
    checks['D: circuit_open_events tripwire fired and paged'] = (
        'circuit_open_events' in breached and 'circuit_open_events' in paged)
    return checks


def main():
    sys.stdout.reconfigure(line_buffering=True)
    repeat = int(sys.argv[sys.argv.index('--repeat') + 1]) if '--repeat' in sys.argv else 1
    base, server = serve()
    print(f"gateway {base} · tiers {httpx.get(f'{base}/health').json()['tiers']} · scratch {os.environ['DATA_DIR']}")
    results = []
    for round_no in range(1, repeat + 1):
        checks = drill(base, round_no)
        results.append(all(checks.values()))
        print(f'\nROUND {round_no} SUMMARY')
        for name, ok in checks.items():
            print(f"  {GREEN + 'PASS' if ok else RED + 'FAIL'}{RESET}  {name}")
    server.should_exit = True
    print(f"\nCUT CABLE: {sum(results)}/{len(results)} rounds passed -> {'PASS' if all(results) else 'FAIL'}")
    return all(results)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)
