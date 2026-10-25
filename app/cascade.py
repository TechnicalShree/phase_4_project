"""Fallback cascade with per-tier circuit breakers and mid-stream hot-swap.

Tiers are keyed by POSITION (0 primary -> 1 secondary -> 2 tertiary), never by model name, so swapping the
model at a position keeps its breaker and a model appearing at two positions cannot share one.
If tier N fails after emitting part of an answer, tier N+1 continues the same stream from that point.
    python -m app.cascade    standalone self-check: reports which tier served which part of each response
"""
import os
import threading
import time


class InjectedFault(ConnectionError):
    """A deliberate failure (the 'cut cable')."""


class AllTiersFailed(RuntimeError):
    pass


class Breaker:
    """closed -> (threshold consecutive failures) -> open -> (cooldown) -> half_open: one trial call."""

    def __init__(self, threshold=None, cooldown_s=None):
        self.threshold = threshold or int(os.getenv('BREAKER_THRESHOLD', '3'))
        self.cooldown_s = cooldown_s if cooldown_s is not None else float(os.getenv('BREAKER_COOLDOWN_S', '30'))
        self.failures, self.opened_at, self.trial = 0, None, False
        self.lock = threading.Lock()

    @property
    def state(self):
        if self.opened_at is None:
            return 'closed'
        return 'half_open' if time.monotonic() - self.opened_at >= self.cooldown_s else 'open'

    def allow(self):
        with self.lock:
            state = self.state
            if state == 'half_open' and not self.trial:
                self.trial = True  # exactly one caller probes a recovering tier
                return True
            return state == 'closed'

    def success(self):
        with self.lock:
            self.failures, self.opened_at, self.trial = 0, None, False

    def failure(self):
        """Returns True when this failure opens (or re-opens) the circuit."""
        with self.lock:
            self.failures += 1
            if self.trial or self.failures >= self.threshold:
                self.opened_at, self.trial = time.monotonic(), False
                return True
            return False


# ponytail: breaker + fault state is per process; share it (Redis) once the gateway runs >1 worker.
BREAKERS = {}
FAULTS = {}  # position -> {'after_chunks': n, 'remaining': count}
_faults_lock = threading.Lock()


def breaker(position):
    return BREAKERS.setdefault(position, Breaker())


def inject(position, after_chunks=0, count=1):
    with _faults_lock:
        FAULTS[position] = {'after_chunks': after_chunks, 'remaining': count}


def clear_faults():
    with _faults_lock:
        FAULTS.clear()


def reset():
    clear_faults()
    BREAKERS.clear()


def _take_fault(position):
    with _faults_lock:
        fault = FAULTS.get(position)
        if not fault or fault['remaining'] <= 0:
            return None
        fault['remaining'] -= 1
        return fault['after_chunks']


def _guarded(position, source):
    """Pass chunks through; an armed fault is thrown INTO the provider so its own span records the failure."""
    cut_at = _take_fault(position)
    for count, chunk in enumerate(source):
        if cut_at is not None and count >= cut_at:
            source.throw(InjectedFault(f'injected failure: tier {position} cable cut after {count} chunks'))
        yield chunk


def stream(providers, messages, offline, start=0, resume=True, on_event=lambda kind, **info: None):
    """Yield (position, chunk). providers[i](messages, offline) -> iterator of text chunks.

    resume=True: hot-swap mid-stream, the next tier continues after the text already emitted.
    resume=False: the next tier restarts from scratch (for structured outputs like JSON).
    """
    emitted = ''
    for position in range(start, len(providers)):
        if not breaker(position).allow():
            on_event('circuit_skip', position=position, state=breaker(position).state)
            continue
        if not resume:
            emitted = ''
        if emitted:
            tier_messages = [*messages, ('ai', emitted),
                             ('human', 'Continue the reply exactly where it stopped. Do not repeat anything.')]
            tier_offline = offline[len(emitted):] if offline.startswith(emitted) else '\n\n' + offline
        else:
            tier_messages, tier_offline = messages, offline
        try:
            for chunk in _guarded(position, providers[position](tier_messages, tier_offline)):
                emitted += chunk
                yield position, chunk
            breaker(position).success()
            return
        except Exception as exc:
            opened = breaker(position).failure()
            on_event('fallback', position=position, error=f'{type(exc).__name__}: {exc}',
                     emitted_chars=len(emitted), next_position=position + 1)
            if opened:
                on_event('circuit_open', position=position, state='open')
    raise AllTiersFailed('every tier failed or is circuit-open')


def _demo():
    def tier(name):
        def provider(messages, offline):
            for word in offline.split(' '):
                yield word + ' '
        return provider

    providers = [tier('primary'), tier('secondary'), tier('tertiary')]
    text = 'one two three four five six seven eight'
    events = []
    log = lambda kind, **info: events.append((kind, info))  # noqa: E731

    def run(label):
        parts = list(stream(providers, [], text, on_event=log))
        served = {}
        for position, chunk in parts:
            served.setdefault(position, []).append(chunk.strip())
        print(f'{label:34} ' + ' | '.join(f"tier {p}: {' '.join(w)}" for p, w in served.items()))
        return parts

    reset()
    os.environ['BREAKER_COOLDOWN_S'] = '0.2'
    healthy = run('healthy')
    assert {p for p, _ in healthy} == {0}

    inject(0, after_chunks=3)
    swapped = run('primary cut after 3 chunks')
    assert [p for p, _ in swapped] == [0, 0, 0, 1, 1, 1, 1, 1]
    assert ''.join(c for _, c in swapped).split() == text.split(), 'hot-swap lost or repeated text'

    inject(0, after_chunks=0, count=2)  # two more consecutive failures -> threshold 3 -> open
    run('primary fails at once (x2)')
    run('primary fails at once (x2)')
    assert breaker(0).state == 'open' and events[-1][0] == 'circuit_open'
    skipped = run('circuit open: primary skipped')
    assert {p for p, _ in skipped} == {1} and events[-1][0] == 'circuit_skip'

    time.sleep(0.25)  # cooldown elapses -> half-open trial succeeds -> closed again
    recovered = run('after cooldown: half-open trial')
    assert {p for p, _ in recovered} == {0} and breaker(0).state == 'closed'

    for kind, info in events:
        print(f'  event {kind:13} {info}')
    print('CASCADE SELF-CHECK: PASS')


if __name__ == '__main__':
    _demo()
