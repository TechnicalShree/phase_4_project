"""SRE-style monitoring daemon: named SLOs over the most recent runs; a breach pages a human.
    python -m app.monitor             one tick (grade new runs, check SLOs, page on breach)
    python -m app.monitor --loop 30   daemon: tick every 30 s
    python -m app.monitor --demo      seed degradations into a scratch store and prove the tripwires fire
"""
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

import httpx

from app import judge, store

WINDOW = int(os.getenv('SLO_WINDOW_RUNS', '20'))
PAGE_COOLDOWN_S = float(os.getenv('PAGE_COOLDOWN_S', '900'))
# name -> (comparison, objective, what it protects)
SLOS = {
    'latency_p95_ms': ('<=', float(os.getenv('SLO_P95_MS', '20000')), 'p95 end-to-end run latency'),
    'error_rate': ('<=', 0.05, 'share of runs that ended in an error'),
    'judge_score_mean': ('>=', 4.0, 'mean judge score (correctness+safety)/2'),
    'judge_score_floor': ('>=', 2.5, 'worst judge score: any single badly graded answer pages'),
    'fallback_rate': ('<=', 0.2, 'share of runs served degraded: a tier failed over or was circuit-skipped'),
    'circuit_open_events': ('<=', 0, 'any tier circuit breaker opening pages'),
}
CASCADE_WINDOW_S = float(os.getenv('CASCADE_WINDOW_S', '900'))


def measure():
    runs = store.query('SELECT * FROM runs ORDER BY created_at DESC LIMIT ?', WINDOW)
    scores = [row['overall'] for row in store.query(
        'SELECT overall FROM judgments WHERE run_id IN (SELECT id FROM runs ORDER BY created_at DESC LIMIT ?)',
        WINDOW)]
    latencies = [r['latency_ms'] for r in runs if r['latency_ms'] is not None]
    since = time.time() - CASCADE_WINDOW_S
    recent_runs = store.query('SELECT COUNT(*) AS n FROM runs WHERE created_at>?', since)[0]['n']
    degraded = store.query("SELECT COUNT(DISTINCT json_extract(detail, '$.run_id')) AS n FROM events WHERE "
                           "kind='cascade' AND name IN ('fallback', 'circuit_skip') AND created_at>?", since)[0]['n']
    opened = store.query("SELECT COUNT(*) AS n FROM events WHERE kind='cascade' AND name='circuit_open' "
                         'AND created_at>?', since)[0]['n']
    return {
        'latency_p95_ms': (statistics.quantiles(latencies, n=100)[94] if len(latencies) > 1
                           else latencies[0] if latencies else None),
        'error_rate': sum(r['status'] == 'error' for r in runs) / len(runs) if runs else None,
        'judge_score_mean': statistics.mean(scores) if scores else None,
        'judge_score_floor': min(scores) if scores else None,
        'fallback_rate': degraded / recent_runs if recent_runs else None,
        'circuit_open_events': opened,
    }


def page(name, value, objective):
    """Page once per SLO per cooldown: pages.log + stderr always, PAGER_WEBHOOK_URL (Slack-style) when set."""
    since = time.time() - PAGE_COOLDOWN_S
    if store.query("SELECT 1 FROM events WHERE kind='page' AND name=? AND created_at>?", name, since):
        return False
    message = f'PAGE [{name}] measured {value:.5g}, objective {objective}'
    store.execute("INSERT INTO events(kind, name, detail) VALUES ('page', ?, ?)", name,
                  json.dumps({'value': value, 'objective': objective}))
    with open(Path(os.getenv('DATA_DIR', 'data')) / 'pages.log', 'a') as log:
        log.write(f'{time.strftime("%Y-%m-%dT%H:%M:%S")} {message}\n')
    print(f'\a!!! {message}', file=sys.stderr)
    if url := os.getenv('PAGER_WEBHOOK_URL'):
        try:
            httpx.post(url, json={'text': message}, timeout=5)
        except httpx.HTTPError as exc:
            print(f'pager webhook failed: {exc}', file=sys.stderr)
    return True


def tick():
    judge.grade_pending()
    report = []
    for name, value in measure().items():
        op, objective, _ = SLOS[name]
        if value is None:
            status = 'NO DATA'
        else:
            status = 'PASS' if (value <= objective if op == '<=' else value >= objective) else 'BREACH'
        paged = status == 'BREACH' and page(name, value, f'{op} {objective}')
        report.append({'slo': name, 'objective': f'{op} {objective:g}', 'value': value, 'status': status,
                       'paged': paged})
    return report


def show(report):
    print(f"{'SLO':20} {'objective':>12} {'measured':>10}  status")
    for row in report:
        value = '-' if row['value'] is None else f"{row['value']:.5g}"
        print(f"{row['slo']:20} {row['objective']:>12} {value:>10}  {row['status']}"
              + ('  -> PAGED' if row['paged'] else ''))
    return {row['slo'] for row in report if row['status'] == 'BREACH'}


def demo():
    os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='monitor-demo-')
    print(f'scratch store: {os.environ["DATA_DIR"]}\n\n== 1. healthy baseline: 5 good runs')
    for _ in range(5):
        judge.seed_run('good', *judge.GOOD, 'account', 'low', judge.FACTS)
    baseline = show(tick())
    print('\n== 2. synthetic degradation: one seeded bad trace (invented account actions)')
    judge.seed_run('bad', *judge.BAD, 'account', 'low', judge.FACTS)
    quality = show(tick())
    print('\n== 3. synthetic degradation: provider errors + slow runs')
    for _ in range(2):
        store.execute("INSERT INTO runs(id, question, status, error, latency_ms) VALUES "
                      "(?, 'seeded error', 'error', 'seeded: upstream 503', 45000)", f'seed-err-{time.time_ns()}')
    outage = show(tick())
    checks = {
        'baseline is healthy': not baseline,
        'bad trace fires judge_score_floor': 'judge_score_floor' in quality,
        'errors fire error_rate': 'error_rate' in outage,
        'slow runs fire latency_p95_ms': 'latency_p95_ms' in outage,
    }
    print('\n== SUMMARY')
    for name, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
    print('MONITOR DEMO: ' + ('PASS' if all(checks.values()) else 'FAIL'))
    return all(checks.values())


if __name__ == '__main__':
    sys.stdout.reconfigure(line_buffering=True)
    from app import tracing
    tracing.setup()
    if '--demo' in sys.argv:
        sys.exit(0 if demo() else 1)
    elif '--loop' in sys.argv:
        interval = float(sys.argv[sys.argv.index('--loop') + 1])
        while True:
            print(time.strftime('\n%H:%M:%S tick'))
            show(tick())
            time.sleep(interval)
    else:
        show(tick())
