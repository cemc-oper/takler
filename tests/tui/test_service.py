"""Unit tests for :class:`takler.tui.service.TaklerTuiService`.

The service is a thin wrapper over ``TaklerServiceClient``; these tests
substitute a recording fake for the client and pin compact query connection
reuse, ``ping``'s never-raise contract, and control-command forwarding.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

import takler.tui.service as service_module
from takler.exceptions import QueryResponseError
from takler.protocol.commands import Command
from takler.tui.service import TaklerTuiService


class FakeTransport:
    def __init__(self) -> None:
        self.calls: List[tuple] = []
        self.output = "SHOW-PAYLOAD"
        self.error: Optional[Exception] = None

    def call(self, command: Command, payload: Dict[str, Any]):
        self.calls.append((command, payload))
        if self.error is not None:
            raise self.error
        return SimpleNamespace(output=self.output)


class FakeInnerClient:
    """Stands in for ``TaklerServiceClient``; records everything."""

    def __init__(self, host, port, connect_config=None, transport_name=None, **_):
        self.host = host
        self.port = str(port)
        self.listen_address = f"{host}:{port}"
        self.connect_config = connect_config
        self.transport_name = transport_name
        self.transport = FakeTransport()
        self.started = 0
        self.shutdowns = 0
        self.control_calls: List[tuple] = []
        self.snapshot_errors: List[Exception] = []
        self.snapshot_calls = 0

    def run_query_snapshot(self, **selection):
        self.snapshot_calls += 1
        if self.snapshot_errors:
            raise self.snapshot_errors.pop(0)
        return selection

    def run_query_detail(self, snapshot, path, groups):
        return snapshot, path, groups

    def start(self) -> None:
        self.started += 1

    def shutdown(self) -> None:
        self.shutdowns += 1

    def run_command_requeue(self, node_path):
        self.control_calls.append(("requeue", node_path))

    def run_command_suspend(self, node_path):
        self.control_calls.append(("suspend", node_path))

    def run_command_resume(self, node_path):
        self.control_calls.append(("resume", node_path))

    def run_command_run(self, node_path, force):
        self.control_calls.append(("run", node_path, force))

    def run_command_force(self, variable_paths, state, recursive):
        self.control_calls.append(("force", variable_paths, state, recursive))

    def run_command_free_dep(self, node_paths, dep_type):
        self.control_calls.append(("free_dep", node_paths, dep_type))


@pytest.fixture
def service(monkeypatch) -> TaklerTuiService:
    monkeypatch.setattr(service_module, "TaklerServiceClient", FakeInnerClient)
    return TaklerTuiService(host="fake-host", port=33083)


def test_address_properties_delegate_to_the_client(service: TaklerTuiService) -> None:
    assert service.host == "fake-host"
    assert service.port == "33083"
    assert service.listen_address == "fake-host:33083"


def test_query_snapshot_opens_and_reuses_connection(
    service: TaklerTuiService,
) -> None:
    first = service.read_snapshot(scope_path="/flow")
    assert first == {"scope_path": "/flow", "depth": None}
    inner = service._inner
    assert inner.started == 1
    assert inner.snapshot_calls == 1
    assert inner.transport.calls == []
    service.read_snapshot(scope_path="/other", depth=1)
    assert inner.started == 1
    assert inner.snapshot_calls == 2


def test_query_snapshot_retries_only_transient_capture_changes(
    service: TaklerTuiService, monkeypatch
) -> None:
    monkeypatch.setattr(service_module, "sleep", lambda _: None)
    service._inner.snapshot_errors = [
        QueryResponseError("resource_exhausted", "query state changed during capture"),
        QueryResponseError("resource_exhausted", "query state is changing"),
    ]
    assert service.read_snapshot(scope_path="/flow") == {
        "scope_path": "/flow",
        "depth": None,
    }
    assert service._inner.snapshot_calls == 3
    service._inner.snapshot_errors = [
        QueryResponseError("resource_exhausted", "snapshot memory limit")
    ]
    with pytest.raises(QueryResponseError):
        service.read_snapshot()
    assert service._inner.snapshot_calls == 4
    assert (
        service.read_detail(None, "/flow", ["parameters"], current=lambda: False)
        is None
    )


def test_ping_reports_elapsed_and_never_raises(service: TaklerTuiService) -> None:
    ok, message = service.ping()
    assert ok is True
    assert message.startswith("pong in ")
    assert service._inner.transport.calls == [(Command.PING, {})]


def test_ping_failure_resets_the_connection(service: TaklerTuiService) -> None:
    service._ensure_open()
    service._inner.transport.error = ConnectionError("gone")
    ok, message = service.ping()
    assert ok is False
    assert "gone" in message
    assert service._connected is False
    # The next call reconnects.
    service._inner.transport.error = None
    assert service.ping()[0] is True
    assert service._inner.started == 2


def test_control_commands_forward_to_the_client(service: TaklerTuiService) -> None:
    service.requeue(["/flow1"])
    service.suspend(["/flow1"])
    service.resume(["/flow1"])
    service.run(["/flow1/task1"], force=True)
    service.force_state(["/flow1/task1"], state="complete", recursive=False)
    service.free_dep(["/flow1/task2"], dep_type="all")

    assert service._inner.control_calls == [
        ("requeue", ["/flow1"]),
        ("suspend", ["/flow1"]),
        ("resume", ["/flow1"]),
        ("run", ["/flow1/task1"], True),
        ("force", ["/flow1/task1"], "complete", False),
        ("free_dep", ["/flow1/task2"], "all"),
    ]


def test_close_shuts_down_only_when_connected(service: TaklerTuiService) -> None:
    service.close()  # never opened: no shutdown
    assert service._inner.shutdowns == 0

    service.read_snapshot()
    service.close()
    assert service._inner.shutdowns == 1
    service.close()  # idempotent
    assert service._inner.shutdowns == 1


def test_context_manager_opens_and_closes(service: TaklerTuiService) -> None:
    with service:
        assert service._inner.started == 1
    assert service._inner.shutdowns == 1
    assert service._connected is False
