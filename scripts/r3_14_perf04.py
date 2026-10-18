"""PERF-04 fixed arrival grid and bounded rotating-task model (no service I/O)."""

from collections import Counter
from dataclasses import dataclass
import random
import itertools

NS = 1_000_000_000
SEED = 20261018
STEPS = (
    ("init", None),
    ("event", "e1"),
    ("meter", 25),
    ("event", "e2"),
    ("meter", 50),
    ("event", "e3"),
    ("meter", 75),
    ("event", "e4"),
    ("meter", 100),
    ("complete", None),
)
RUNS = {
    "S": dict(tasks=1000, dispersed=False, seconds=60, rate=10, burst=False),
    "V": dict(tasks=1000, dispersed=False, seconds=180, rate=100, burst=True),
    "H": dict(tasks=10000, dispersed=False, seconds=180, rate=100, burst=True),
    "D": dict(tasks=10000, dispersed=True, seconds=180, rate=100, burst=True),
    "L": dict(tasks=10000, dispersed=True, seconds=180, rate=100, burst=True),
    "R": dict(tasks=10000, dispersed=True, seconds=180, rate=100, burst=True),
}


class Grid:
    """Integer monotonic grid, including the actual next slot at rate boundaries.

    poll returns a half-open expired index range and at most ONE live slot.
    Expired slots have no retained per-slot objects. The caller records the range.
    """

    def __init__(self, start_ns, seconds=180, rate=100, burst=True):
        self.start_ns = start_ns
        self.segments = []
        index = 0
        windows = [(0, 60, rate), (60, 70, 1000), (70, seconds, rate)]
        if not burst:
            windows = [(0, seconds, rate)]
        for lo, hi, hz in windows:
            if hi <= lo or NS % hz:
                raise ValueError("invalid fixed grid")
            count = (hi - lo) * hz
            self.segments.append((index, index + count, start_ns + lo * NS, NS // hz))
            index += count
        self.total, self.cursor = index, 0
        self.end_ns = start_ns + seconds * NS

    def at(self, index):
        if index == self.total:
            return self.end_ns
        for lo, hi, start, period in self.segments:
            if lo <= index < hi:
                return start + (index - lo) * period
        raise IndexError(index)

    def poll(self, now_ns):
        due = -1
        for lo, hi, start, period in self.segments:
            if now_ns >= start:
                due = min(hi - 1, lo + (now_ns - start) // period)
        due = min(due, self.total - 1)
        old = self.cursor
        if now_ns >= self.end_ns:
            self.cursor = self.total
            return (old, self.total), None
        if due < old:
            return (old, old), None
        self.cursor = due + 1
        return (old, due), due


def candidates(size, dispersed):
    if size not in (1000, 10000) or dispersed and size != 10000:
        raise ValueError("PERF-04 only permits 1k/10k and eight-flow 10k dispersion")
    rng = random.Random(SEED)
    counts = [32, 32, 31, 31, 31, 31, 31, 31] if dispersed else [250]
    by_flow = [
        [f"/flow{f:03d}/task{n:04d}" for n in rng.sample(range(999), count)]
        for f, count in enumerate(counts)
    ]
    # Interleave flows before assigning 25 tasks to each of the ten stages.
    pool = [row[i] for i in range(max(counts)) for row in by_flow if i < len(row)]
    sentinel = f"/flow{8 if size == 10000 else 0:03d}/task0999"
    return pool, sentinel


@dataclass(frozen=True)
class Reservation:
    serial: int
    task: int
    stage: int
    attempt: str
    operation: str


@dataclass
class SlotTask:
    path: str
    stage: int
    attempt: str
    busy: bool = False
    unknown: bool = False
    reservation: Reservation | None = None


class Pool:
    """One current identity and reservation per task; no historical callback map."""

    def __init__(self, paths, identities=None):
        if len(paths) != 250 or len(set(paths)) != 250:
            raise ValueError("exactly 250 distinct pool tasks required")
        self.tasks = [
            SlotTask(
                p, i // 25, identities[p]["attempt"] if identities else f"initial-{i}"
            )
            for i, p in enumerate(paths)
        ]
        self.next = [0] * 12
        self.serial = itertools.count(1)

    def reserve(self, stage):
        for offset in range(250):
            i = (self.next[stage] + offset) % 250
            task = self.tasks[i]
            if task.stage == stage and not task.busy and not task.unknown:
                operation = (
                    STEPS[stage][0]
                    if stage < 10
                    else "requeue"
                    if stage == 10
                    else "run"
                )
                token = Reservation(
                    next(self.serial), i, stage, task.attempt, operation
                )
                task.busy, task.reservation = True, token
                self.next[stage] = (i + 1) % 250
                return token
        return None

    def current(self, token):
        if not isinstance(token, Reservation) or not 0 <= token.task < 250:
            return False
        task = self.tasks[token.task]
        return (
            not task.unknown
            and task.busy
            and task.reservation == token
            and task.stage == token.stage
            and task.attempt == token.attempt
        )

    def cancel_unsent(self, token):
        if not self.current(token):
            return False
        task = self.tasks[token.task]
        task.busy, task.reservation = False, None
        return True

    def finish(self, token, *, success, attempt=None):
        if not self.current(token):
            return False
        task = self.tasks[token.task]
        if not success:
            task.unknown, task.reservation = True, None
            return True
        if token.stage == 11:
            if not attempt or attempt == task.attempt:
                raise ValueError("rotation must establish a new attempt")
            task.attempt, task.stage = attempt, 0
        else:
            task.stage += 1
        task.busy, task.reservation = False, None
        return True


def prefix(stage):
    return dict(
        status="submitted" if stage == 0 else "complete" if stage == 10 else "active",
        events=[
            any(op == "event" and value == f"e{n}" for op, value in STEPS[:stage])
            for n in range(1, 5)
        ],
        meter=max([0, *(v for op, v in STEPS[:stage] if op == "meter")]),
    )


def simulate(delay_ns=0, *, pauses=()):
    """Virtual-time child regression, with a coarse two-delay rotation model.

    This does not validate the independent R process or its 16-pipeline limit.
    """
    import heapq

    grid = Grid(0)
    pool = Pool(candidates(1000, False)[0])
    pending, counts = [], Counter()
    now, seq, pause_index = 0, 0, 0
    while grid.cursor < grid.total:
        if pause_index < len(pauses) and now >= pauses[pause_index][0]:
            now = pauses[pause_index][1]
            pause_index += 1
        while pending and pending[0][0] <= now:
            _, _, token, control = heapq.heappop(pending)
            pool.finish(
                token,
                success=True,
                attempt=f"rotated-{seq}" if token.stage == 11 else None,
            )
            seq += 1
            if pool.tasks[token.task].stage in (10, 11):
                stage = pool.tasks[token.task].stage
                pool.next[stage] = token.task
                rotation = pool.reserve(stage)
                heapq.heappush(pending, (now + delay_ns, seq, rotation, True))
            counts["control_completed" if control else "completed"] += 1
        expired, index = grid.poll(now)
        counts["offered"] += expired[1] - expired[0]
        counts["expired"] += expired[1] - expired[0]
        if index is not None:
            counts["offered"] += 1
            i = pool.reserve(index % 10)
            if i is None:
                counts["busy"] += 1
            else:
                counts["sent"] += 1
                seq += 1
                heapq.heappush(pending, (now + delay_ns, seq, i, False))
        now = grid.at(grid.cursor)
    counts["not_sent"] = counts["expired"] + counts["busy"]
    counts["pending"] = sum(not control for _, _, _, control in pending)
    return counts
