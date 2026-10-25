# Campus IT Swarm — Phase 4: Productionizing Agents

**Chosen domain: Option 1 — productionize my Phase 3 project (Campus IT Helpdesk).** I rebuilt it as a lean
4-agent LangGraph swarm over the same domain and the same fictional campus data. On top of it are a streaming
gateway, tracing, LLM-as-judge grading, SLO monitoring with paging, a semantic cache and model router, and a
fallback cascade with circuit breakers. A deliberate "Cut Cable" failure drill proves the resilience layer.

**Submission option B — GitHub repo only.** Local infra is required, so read this first:

- **Python 3.12.**
- **Arize Phoenix** runs locally from the pip install (`phoenix serve`). **No Docker.**
- **Ollama is optional.** It's only needed for `MODEL_MODE=live` with local models.
- **Default `MODEL_MODE=demo` needs no keys, no GPU and no network.** The one exception is a one-time ~70 MB
  embedding-model download for the cache.

All campus records and service states are fictional.

---

## 1. Run it (≈10 minutes on a fresh clone)

```sh
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt            # largest item is arize-phoenix
cp .env.example .env                       # MODEL_MODE=demo by default

phoenix serve                              # terminal 1 — tracing UI on http://localhost:6006
uvicorn app.api:app --port 8000            # terminal 2 — gateway (first boot downloads the embedding model)
```

Then run the self-contained proofs. None of them needs the gateway or Phoenix to be running already.

| Command | What it proves | Ends with |
|---|---|---|
| `python -m pytest tests` | sync, SSE order, async job, cache hit, mid-stream hot-swap, cascade | `6 passed` |
| `python -m app.swarm "Wi-Fi down in North Hall"` | the swarm runs from plain Python (step 1) | final state |
| `python -m app.judge --seed` | judge scores a seeded good vs bad trace differently | `PASS: bad trace scored lower` |
| `python -m app.monitor --demo` | seeded degradations fire the right tripwires and page | `MONITOR DEMO: PASS` |
| `python scripts/cache_demo.py` | identical and paraphrase hits, different-building miss, account-ID guard | `CACHE DEMO: PASS` |
| `python -m app.cascade` | cascade standalone: which tier served which part; breaker open/skip/recover | `CASCADE SELF-CHECK: PASS` |
| `python scripts/cut_cable.py --repeat 3` | **the failure drill** over real HTTP/SSE (section 4) | `CUT CABLE: 3/3 rounds passed -> PASS` |

`TRACING=0` silences span export if Phoenix isn't running.

### Live models (optional)

Set `MODEL_MODE=live` in `.env`. Tiers are `kind:model` specs; `kind` is `openrouter | openai | ollama | vllm`.

```dotenv
MODEL_MODE=live
TIER_0=openrouter:openai/gpt-4.1-mini        # frontier / primary  (needs OPENROUTER_API_KEY)
TIER_1=ollama:qcwind/qwen3-8b-instruct-Q4-K-M:latest   # utility / secondary: 8B, Q4_K_M quantized, local
# Position 2 is always the static, KB-grounded safe answer: it cannot fail.
```

The evidence in `docs/evidence/*_live*.txt` was produced with `TIER_0=ollama:gemma4:26b` and
`TIER_1=ollama:qcwind/qwen3-8b-instruct-Q4-K-M:latest`. Both are real local models served by Ollama.

---

## 2. Honesty table: what is live-wired vs standalone

"Live-wired" means it runs inside a normal `curl` request to the gateway. "Standalone" means it is a separate
process or script that you run on purpose.

| Capability | Status | Will a plain `curl` to the gateway show it? |
|---|---|---|
| Sync / SSE / async-job endpoints | **Live-wired** | Yes: `/analyze`, `/analyze/stream`, `/analyze/async` + `/jobs/{id}` |
| OpenTelemetry → Phoenix tracing | **Live-wired** for every gateway request | Yes, in the Phoenix UI (if `phoenix serve` is running); not in the response |
| Semantic cache + cost model | **Live-wired** in front of all three endpoints | Yes: send the same question twice; `"cached": true`, `/stats` → `cache` |
| Model router + budget | **Live-wired** for every model call | Yes: `/stats` → `routes`, `budget_usd_24h`; per-call model in the trace |
| Fallback cascade + circuit breaker | **Live-wired** for every model call. Unlike the reference project, it is spliced into the graph. | Only during a failure. Healthy runs never fall back. To see it, enable `ENABLE_FAULT_INJECTION=1`, arm `/admin/fault`, then call `/analyze/stream` (section 4). `/analyze` also falls back, but you see only the finished answer. |
| Mid-stream hot-swap | **Live-wired** for the `writer` agent (the only streamed output) | Yes, with an armed fault: tokens tagged `tier: 0` then `tier: 1` plus a `fallback` event on the same SSE connection |
| Structured calls (triage, specialist, judge) | **Live-wired**, but they **restart** on the next tier instead of resuming | A half-written JSON object can't be continued safely, so the partial output is discarded |
| LLM-as-judge | **Standalone / offline.** It is not in the request path. | No. It runs via `python -m app.judge` or on every monitor tick. |
| Monitoring daemon + paging | **Standalone process** (`python -m app.monitor --loop 30`) | No. Pages go to `data/pages.log`, stderr, and optionally `PAGER_WEBHOOK_URL` (Slack-style). There is no real PagerDuty account. |
| Fault injection | Live endpoint, **off by default** | `POST /admin/fault` returns 403 unless `ENABLE_FAULT_INJECTION=1` |

Smaller simplifications, stated plainly:

- **Demo mode uses no LLM.** `MODEL_MODE=demo` uses scripted test doubles that stream a deterministic,
  KB-grounded answer word by word. In demo mode the "judge" is a heuristic double. Everything else (graph,
  cache, router, cascade, breaker, tracing, monitor) is the same code in both modes.
- **The researcher agent picks its tools deterministically**, based on category and account ID. The other three
  agents make model calls.
- **Costs are estimates.** Tokens are counted as characters ÷ 4. Local tiers have no real invoice, so they are
  priced notionally per tier position (`TIER_0_USD_PER_MTOK`, `TIER_1_USD_PER_MTOK`) to give the cost model
  something to save.
- **The judge reads runs from the SQLite run log**, not from the Phoenix query API. Each row carries its Phoenix
  `trace_id`/`span_id`, and scores are written back to Phoenix as span annotations.
- **Single-process limits.** Breaker state, fault state and the async queue are per process, so run one uvicorn
  worker. Jobs are stored in SQLite. A job that was running when the server restarted is marked `failed`, not
  resumed.
- **vLLM is not exercised** (no CUDA GPU here). The `vllm:` tier kind points at any `vllm serve` endpoint, and
  quantization is demonstrated with the Q4_K_M 8B model served by Ollama/llama.cpp.
- **Phase 3's human-in-the-loop ticket writes and guardrails are not carried over.** That was Phase 3's topic.

---

## 3. Build history: one step per commit, and why in this order

`git log --reverse --oneline` shows the progression. Each step adds one concern and removes nothing.

| Step | Commit | Capability added | Why it had to come at this point |
|---|---|---|---|
| 1 | `2453c5b` | 4-agent swarm: triage → researcher (3 tools) → specialist → writer | Something worth productionizing must exist and run from plain Python first |
| 2 | `3834781` | FastAPI gateway, sync `/analyze`, SQLite run log | One stable entry point before adding more ways in; the run log becomes the judge and monitor input |
| 3 | `76f15ab` | SSE `/analyze/stream` with typed events | Users see progress; later layers (fallback, cache hit) need a channel to announce themselves |
| 4 | `d022a81` | Async jobs: 202 + `/jobs/{id}` | Long agent runs shouldn't hold a connection open; results survive client disconnects |
| 5 | `96cb8cb` | OTel → Phoenix: run root, agent, tool, LLM spans | You can't grade, monitor or debug fallbacks you can't see; every later step is checked in traces |
| 6 | `bee922b` | LLM-as-judge (correctness, safety) + Phoenix annotations | Monitoring needs a quality signal, not only latency and errors |
| 7 | `cf3eb0a` | Monitor daemon: named SLOs, tripwires, paging | Before optimizing cost or adding resilience, we must detect regressions caused by those changes |
| 8 | `b0ad696` | Semantic cache + cost model | Cheapest win first; measured against the SLOs already in place |
| 9 | `fc9fe6b` | Router: per-agent cost tiers + budget | Route expensive models only where they matter; the tier list it creates is what the cascade falls through |
| 10 | `c48174b` | Position-keyed cascade + circuit breaker + mid-stream hot-swap | Only now is there something worth protecting, and a monitor to watch it |
| 11 | `c1b30ec` | Cut Cable drill, fault endpoints, cascade tripwires | Resilience is only real once proven under a deliberate failure and watched by the monitor |
| 12 | latest | Tests, evidence, this README | The honesty audit |

The cascade is keyed by **tier position**, not by model string. This avoids the reference project's documented bug.
Swapping the model at a position keeps that position's breaker, and the same model at two positions can't share one.

---

## 4. The failure drill ("Cut Cable")

`python scripts/cut_cable.py --repeat 3` starts the real gateway on a real TCP port with a scratch data dir, then:

- **A. Hot-swap.** Arms a fault: tier 0 dies after 6 chunks of the writer's answer. It opens **one** SSE connection
  and checks:
  - HTTP 200
  - the first 6 tokens come from tier 0 and the rest from tier 1
  - a `fallback` event arrives in-stream
  - the stream ends with `final`
  - tier-0 chunks plus tier-1 chunks equal the stored answer exactly (nothing lost or repeated)
- **B. Circuit breaker.** Tier 0 fails outright twice more (threshold 3), so the circuit opens. The next request
  gets `circuit_skip`, and the database shows **no tier-0 call** for that run.
- **C. Recovery.** After the cooldown, a half-open trial goes to tier 0, succeeds, and the circuit closes.
- **D. Monitor.** One daemon tick over the drill traffic: `fallback_rate` and `circuit_open_events` breach and page.

Passing transcripts: [demo mode, 3/3 rounds](docs/evidence/11_cut_cable_demo.txt) and
[real models, 2/2 rounds](docs/evidence/11b_cut_cable_live_models.txt). In the real-model run, gemma4:26b is cut
after "Diagnosis: The Wi-Fi" and qwen3-8b continues "service is degraded…" in the same response.

To do it by hand with curl (gateway started with `ENABLE_FAULT_INJECTION=1`):

```sh
curl -s -X POST localhost:8000/admin/fault -H 'Content-Type: application/json' \
     -d '{"position": 0, "after_chunks": 6, "count": 1}'
curl -sN -X POST localhost:8000/analyze/stream -H 'Content-Type: application/json' \
     -d '{"question": "Wi-Fi outage in North Hall", "no_cache": true}'
curl -s localhost:8000/stats      # breakers, armed_faults, cache, routes, budget
```

---

## 5. Architecture

```
            ┌───────────────────────── FastAPI gateway (app/api.py) ─────────────────────────┐
 client ──► │ POST /analyze  · POST /analyze/stream (SSE) · POST /analyze/async → GET /jobs/{id} │
            │        │                                                                         │
            │        ▼                                                                         │
            │  semantic cache (app/cache.py) ── hit ──► cached answer + cache_hit event        │
            │        │ miss                                                                    │
            │        ▼                                                                         │
            │  LangGraph swarm (app/swarm.py)                                                  │
            │   triage ──► researcher ──► specialist ──► writer (streams tokens)               │
            │   (utility)  (3 tools)      (utility)      (frontier)                            │
            │        │ every model call                                                        │
            │        ▼                                                                         │
            │  router (app/llm.py): agent → cost tier → start position, budget check           │
            │        ▼                                                                         │
            │  cascade (app/cascade.py): tier0 ─✗─► tier1 ─✗─► tier2 static                    │
            │        per-position circuit breaker · mid-stream hot-swap · fault injection      │
            └────────┬───────────────────────────────────────┬────────────────────────────────┘
                     │ OTel spans                             │ runs, llm_calls, events, jobs, cache
                     ▼                                        ▼
              Arize Phoenix :6006  ◄── span annotations ── SQLite data/ops.sqlite
                                                              ▲
                              judge (app/judge.py) ───────────┤ grades runs
                              monitor daemon (app/monitor.py) ┘ SLOs → pages.log / stderr / webhook
```

| File | Role |
|---|---|
| `app/domain.py` | fictional campus accounts, KB, service status (from Phase 3) |
| `app/swarm.py` | the 4-agent LangGraph graph; emits `agent`/`tool_call`/`token` events |
| `app/llm.py` | tier specs, providers, router, budget, cost recording, cascade splice, LLM spans |
| `app/cascade.py` | fallback cascade, circuit breaker, fault injection, standalone self-check |
| `app/api.py` | gateway endpoints, run logging, `/stats`, `/admin/fault` |
| `app/events.py` | typed SSE event schemas (pydantic discriminated union) |
| `app/tracing.py` | Phoenix/OpenInference setup, span helpers |
| `app/cache.py` | embeddings, similarity lookup, cost model stats |
| `app/judge.py` | LLM-as-judge, heuristic double, seeded good/bad traces |
| `app/monitor.py` | SLOs, tick, paging, `--demo` |
| `scripts/cut_cable.py` | failure drill |
| `scripts/cache_demo.py`, `scripts/trace_tree.py` | cache proof, Phoenix trace printer |

### SSE event types

| `event:` | Payload |
|---|---|
| `run` | `run_id` |
| `agent` | `agent` (handoff into triage / researcher / specialist / writer) |
| `tool_call` | `tool`, `args`, `result` |
| `token` | `text`, `tier` (position), `model` |
| `fallback` | `agent`, `position` that failed, `error`, `emitted_chars`, `next_position` |
| `circuit_open` / `circuit_skip` | `agent`, `position`, `state` |
| `cache_hit` | `similarity`, `cached_question`, `saved_usd` |
| `final` / `error` | `result` (run_id, trace_id, answer, category, severity, cost_usd, latency_ms, cached) |

### SLOs (monitor)

| SLO | Objective | Protects |
|---|---|---|
| `latency_p95_ms` | ≤ 20000 (`SLO_P95_MS`) | p95 end-to-end latency over the last 20 runs |
| `error_rate` | ≤ 0.05 | share of runs that ended in an error |
| `judge_score_mean` | ≥ 4.0 | mean of (correctness + safety) / 2 |
| `judge_score_floor` | ≥ 2.5 | any single badly graded answer pages |
| `fallback_rate` | ≤ 0.2 | share of recent runs served degraded (fell back or skipped a tier) |
| `circuit_open_events` | ≤ 0 | any breaker opening pages |

---

## 6. Tech stack

| Layer | Choice |
|---|---|
| Agents | LangGraph 1.2, langchain-openai (OpenAI-compatible client for every tier) |
| Gateway | FastAPI, uvicorn, SSE via `StreamingResponse` |
| Tracing | OpenTelemetry SDK, OpenInference LangChain instrumentor, Arize Phoenix 20 |
| Models | OpenRouter / OpenAI / Ollama / vLLM endpoints; Q4_K_M-quantized qwen3-8B via Ollama locally |
| Cache | fastembed `BAAI/bge-small-en-v1.5` (local ONNX), cosine similarity with numpy |
| Storage | SQLite (WAL): runs, jobs, llm_calls, judgments, events, cache |
| Tests | pytest + FastAPI TestClient |

## 7. Evidence

[`docs/evidence/`](docs/evidence/) holds output captured from real runs:
- live-model sync, SSE (with arrival timestamps) and async-job transcripts
- an 18-span Phoenix trace tree
- judge scores (heuristic and real LLM)
- the monitor degradation demo
- cache hit timing (23.6 s → 0.044 s on an identical re-send)
- router tier swaps
- cascade self-check, drill transcripts and pytest output
