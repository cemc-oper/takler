"""Scale and clean-process checks for the controlled RUN path."""

from unittest.mock import Mock

from takler.logging import configure
from takler.server import batch
from takler.server.scheduler import Scheduler
from tests.unit.server.test_run_effect_fast_path import (
    controlled_logging as controlled_logging,
)


def test_250_normal_fixture_run_init_pairs_take_zero_full_snapshots(monkeypatch):
    import asyncio
    import sys
    import time
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from mix100k01 import NS, fixture, prepare

    configure(level="WARNING")
    try:
        bunch, paths = fixture(1000)
        scheduler = Scheduler(bunch)
        snapshot = Mock(wraps=batch._snapshot)
        monkeypatch.setattr(batch, "_snapshot", snapshot)
        credentials = asyncio.run(
            prepare(
                scheduler,
                paths,
                lambda *args, **kwargs: None,
                time.monotonic_ns() + 30 * NS,
            )
        )
        assert len(credentials) == 250
        assert snapshot.call_count == 0
    finally:
        configure(level="INFO")


def test_core_hook_replacement_before_scheduler_import_keeps_compatibility():
    import os
    from pathlib import Path
    import subprocess
    import sys

    code = """
from takler.core import Bunch, Task
from takler.exceptions import FlowStateError
from takler.logging import configure
configure(level="WARNING")
other = None
def early_hook(self):
    other.state.suspended = True
    raise FlowStateError("changed another task")
Task.before_run = early_hook
from takler.server.scheduler import Scheduler
from takler.server.run_effect import controlled_run
from takler.protocol.commands import RunCommand
b = Bunch()
f = b.add_flow("f")
a = f.add_task("a")
other = f.add_task("b")
f.begin()
s = Scheduler(b)
assert controlled_run(s, a.node_path) is None
reply = s.run_command_run(RunCommand(node_paths=[a.node_path]))
assert reply.results[0].effect == "partial"
assert other.state.suspended
"""
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[3] / "src"))
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_logging_hook_before_core_import_is_not_blessed_as_builtin():
    import os
    from pathlib import Path
    import subprocess
    import sys

    code = """
from takler.logging.backends import NamedLogger
from takler.exceptions import UnsupportedValueError
from takler.logging import configure
configure(level="WARNING")
armed = False
other = None
def log_hook(self, *args, **kwargs):
    if armed:
        other.state.suspended = True
        raise UnsupportedValueError("changed another task")
NamedLogger.info = log_hook
from takler.core import Bunch
from takler.server.scheduler import Scheduler
from takler.server.run_effect import controlled_run
from takler.protocol.commands import RunCommand
b = Bunch()
f = b.add_flow("f")
a = f.add_task("a")
other = f.add_task("b")
f.begin()
a.run()
prior = a.attempt_id
s = Scheduler(b)
armed = True
assert controlled_run(s, a.node_path) is None
assert not other.state.suspended
reply = s.run_command_run(RunCommand(node_paths=[a.node_path], force=True))
assert reply.results[0].effect == "partial", reply
assert other.state.suspended and a.attempt_id == prior
"""
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[3] / "src"))
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
