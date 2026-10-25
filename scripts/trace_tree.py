"""Print one trace from the running Phoenix instance as a span tree (evidence that real spans exist).
    python scripts/trace_tree.py <trace_id>      (default: the latest gateway run in data/ops.sqlite)
"""
import os
import sys
from datetime import datetime

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import store  # noqa: E402
from app.tracing import PHOENIX, PROJECT  # noqa: E402

trace_id = sys.argv[1] if len(sys.argv) > 1 else store.query(
    "SELECT trace_id FROM runs WHERE id NOT LIKE 'seed-%' ORDER BY created_at DESC LIMIT 1")[0]['trace_id']
spans = [s for s in httpx.get(f'{PHOENIX}/v1/projects/{PROJECT}/spans', params={'limit': 1000}).json()['data']
         if s['context']['trace_id'] == trace_id]
children, ids = {}, {s['context']['span_id'] for s in spans}
for span in spans:
    children.setdefault(span['parent_id'], []).append(span)


def show(span, depth):
    attrs = span.get('attributes', {})
    ms = (datetime.fromisoformat(span['end_time']) - datetime.fromisoformat(span['start_time'])).total_seconds() * 1000
    extra = ' '.join(f'{k}={attrs[k]}' for k in ('llm.model_name', 'route.cost_tier', 'tier.position',
                                                 'cascade.served_by') if k in attrs)
    print(f"{'  ' * depth}{span['name']:<{40 - 2 * depth}} {span['span_kind']:<8} {ms:8.0f} ms  "
          f"{span['status_code']:<6} {extra}")
    for child in sorted(children.get(span['context']['span_id'], []), key=lambda s: s['start_time']):
        show(child, depth + 1)


print(f'trace {trace_id} from {PHOENIX} (project {PROJECT})')
for root in (s for s in spans if s['parent_id'] not in ids):
    show(root, 0)
print(f'\n{len(spans)} spans in this trace')
