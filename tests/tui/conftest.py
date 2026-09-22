"""Shared fixtures for the TUI test suite.

The whole suite runs against an in-memory :class:`~takler.core.Bunch`
serialised exactly the way the server's ``show`` command produces it
(``json.dumps(bunch.to_dict())``), so ``parse_show`` and every widget
downstream of it see the same payload shape as in production.

:class:`FakeTuiService` is the test double for
:class:`~takler.tui.service.TaklerTuiService`: it returns the canned
payload for ``show`` and records every control call, so pilot tests can
assert which command a key press or menu pick would have sent to the
server without any network.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Tuple

import pytest

from takler.core import Bunch, Flow
from takler.core.repeat import RepeatDate
from takler.tui.show_parser import ShowSnapshot, parse_show


def build_rich_bunch() -> Bunch:
    """A bunch exercising every attribute kind the TUI renders.

    Layout::

        flow1 (FLOW_HOME=/flow)
        ├── family1 (limit big 2)
        │   ├── task1 (TAKLER_HOME/TAKLER_SCRIPT, trigger, complete-trigger,
        │   │          repeat, time, event, meter; suspended)
        │   └── task2 (in-limit big)
        └── task3 (bare leaf)
    """
    bunch = Bunch(name="test_bunch")

    flow = bunch.add_flow(Flow("flow1"))
    flow.add_parameter("FLOW_HOME", "/flow")

    family = flow.add_container("family1")
    family.add_limit("big", 2)

    task1 = family.add_task("task1")
    task1.add_parameter("TAKLER_HOME", "/tmp/takler_home")
    task1.add_parameter("TAKLER_SCRIPT", "/tmp/takler_home/flow1/family1/task1.takler")
    task1.add_trigger("./task2 == complete")
    task1.add_complete_trigger("./task2:event_done")
    task1.add_repeat(RepeatDate("YMD", 20240101, 20240131, 1))
    task1.add_time("12:00")
    task1.add_event("evt")
    task1.add_meter("mtr", 0, 10)
    task1.suspend()

    task2 = family.add_task("task2")
    task2.add_in_limit("big")

    flow.add_task("task3")

    return bunch


def show_payload(bunch: Bunch) -> str:
    """Serialise ``bunch`` the way ``Scheduler.handle_request_show`` does."""
    return json.dumps(bunch.to_dict())


@pytest.fixture
def rich_bunch() -> Bunch:
    return build_rich_bunch()


@pytest.fixture
def rich_payload(rich_bunch: Bunch) -> str:
    return show_payload(rich_bunch)


@pytest.fixture
def snapshot(rich_payload: str) -> ShowSnapshot:
    return parse_show(rich_payload)


class FakeTuiService:
    """Records commands instead of sending them; ``show`` returns a fixture.

    The interface mirrors :class:`takler.tui.service.TaklerTuiService` --
    the app only ever talks to these members, so the double is a drop-in.
    """

    def __init__(self, payload: str, listen_address: str = "fake-host:33083"):
        self._payload = payload
        self.listen_address = listen_address
        self.show_calls: List[Dict[str, Any]] = []
        self.calls: List[Tuple[str, Dict[str, Any]]] = []
        self.ping_result: Tuple[bool, str] = (True, "pong in 0:00:00.000001")
        self.closed = False

    # -- Queries -------------------------------------------------

    def show(self, **flags: Any) -> str:
        self.show_calls.append(flags)
        return self._payload

    def ping(self) -> Tuple[bool, str]:
        self.calls.append(("ping", {}))
        return self.ping_result

    # -- Control commands -----------------------------------------

    def _record(self, name: str, **kwargs: Any) -> None:
        self.calls.append((name, kwargs))

    def requeue(self, paths: List[str]) -> None:
        self._record("requeue", paths=paths)

    def suspend(self, paths: List[str]) -> None:
        self._record("suspend", paths=paths)

    def resume(self, paths: List[str]) -> None:
        self._record("resume", paths=paths)

    def run(self, paths: List[str], force: bool = False) -> None:
        self._record("run", paths=paths, force=force)

    def force_state(
        self, paths: List[str], state: str, recursive: bool = False
    ) -> None:
        self._record("force_state", paths=paths, state=state, recursive=recursive)

    def free_dep(self, paths: List[str], dep_type: str = "all") -> None:
        self._record("free_dep", paths=paths, dep_type=dep_type)

    # -- Lifecycle -------------------------------------------------

    def close(self) -> None:
        self.closed = True

    def names(self) -> List[str]:
        """Control/query command names in call order (``show`` excluded)."""
        return [name for name, _ in self.calls]


@pytest.fixture
def fake_service(rich_payload: str) -> FakeTuiService:
    return FakeTuiService(rich_payload)


async def wait_until(
    predicate: Callable[[], bool],
    pilot,
    *,
    timeout: float = 5.0,
    message: str = "condition not met",
) -> None:
    """Pump the pilot until ``predicate`` holds.

    App actions run on ``@work(thread=True)`` workers whose results are
    marshalled back via ``call_from_thread``; a single ``pilot.pause()``
    can observe the UI mid-flight, so pilot tests poll here instead of
    guessing how many pauses a given action needs.
    """
    import asyncio

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"{message} (timed out after {timeout}s)")
        await pilot.pause(0.02)


def text_of(static) -> str:
    """Plain-text content of a ``Static`` across renderable kinds."""
    rendered = static.render()
    plain = getattr(rendered, "plain", None)
    return plain if plain is not None else str(rendered)
