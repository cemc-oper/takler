"""Job script permission bits and the startup umask check.

Two related halves of the same deployment concern: a job script exports
``TAKLER_PASS``, so who may read it decides whether authentication buys
anything.

* ``create_job_script`` must not set the read/write bits itself. It only adds
  the owner execute bit, leaving the read/write bits exactly as the process
  umask made them (Requirements 12.6, 16.22). Both halves are asserted under
  umask ``0022`` (the common default, group/other readable) and ``0077``
  (owner only), so the test would fail either if takler forced a fixed mode
  such as ``0o755`` or if it dropped the execute bit.
* ``TaklerServer.start()`` must emit exactly one WARNING when Auth_Mode is
  enabled and the umask leaves new files readable to others, and none when the
  umask is narrow or Auth_Mode is disabled (Requirement 12.8).

The umask is per-process state, so every test that changes it restores the
previous value in a fixture ``finally``.

Validates: Requirements 12.6, 12.8, 16.22
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import os
import stat
import struct
import subprocess
import sys
from pathlib import Path
from typing import Iterator
from unittest.mock import Mock

import pytest

import takler.logging
from takler.core import Bunch, Flow, NodeStatus
from takler.server import TaklerServer
from takler.server.connect_config import generate_connect_config
from takler.exceptions import JobSubmissionError
from takler.tasks import ShellScriptTask
from takler.tasks.shell.constant import TAKLER_JOB
from takler.tasks.shell import shell_render
from takler.tasks.shell.shell_runner import ShellRunner


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="tests for linux only")


# Read and write bits of all three permission classes: the bits takler must
# leave to the umask.
_RW_BITS = 0o666

# The distinctive part of the startup WARNING text. Counting occurrences of
# ``"umask"`` would over-count: the single message names the word twice.
_WARNING_MARKER = "lets users other than the owner read"


@pytest.fixture
def with_umask(request) -> Iterator[int]:
    """Set the process umask for one test and restore it afterwards.

    Indirectly parameterized: ``request.param`` is the umask to install. The
    previous value is restored in ``finally`` so a failing assertion cannot
    leak a modified umask into the rest of the session.
    """
    wanted = request.param
    previous = os.umask(wanted)
    try:
        yield wanted
    finally:
        os.umask(previous)


def _make_task(tmp_path: Path) -> ShellScriptTask:
    """A single-task flow whose script renders to ``tmp_path``."""
    script_dir = tmp_path / "scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    script_path = script_dir / "task1.takler"
    script_path.write_text("echo hello\n")

    bunch = Bunch(host="login01", port="33083")
    bunch.add_parameter("TAKLER_HOME", str(tmp_path / "takler_home"))
    with Flow("flow1") as flow1:
        bunch.add_flow(flow1)
        task1 = flow1.add_task(ShellScriptTask("task1", str(script_path)))
    return task1


# ---------------------------------------------------------------------------
# Job script permissions (Requirements 12.6, 16.22)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("with_umask", [0o022, 0o077, 0o002], indirect=True)
def test_job_script_is_owner_executable_and_keeps_umask_read_write_bits(
    tmp_path, with_umask
):
    task = _make_task(tmp_path)

    task.create_job_script()

    job_script_path = Path(task.find_parameter(TAKLER_JOB).value)
    assert job_script_path.is_file()
    job_mode = job_script_path.stat().st_mode

    # A plain file created in the same directory under the same umask is the
    # reference: whatever read/write bits it gets, the job script must have.
    reference_path = job_script_path.parent / "reference"
    reference_path.write_text("")
    reference_mode = reference_path.stat().st_mode

    assert job_mode & _RW_BITS == reference_mode & _RW_BITS
    assert job_mode & stat.S_IXUSR


@pytest.mark.parametrize("mode", [0o640, 0o664, 0o755])
def test_overwrite_keeps_inode_and_only_adds_owner_execute(tmp_path, mode):
    task = _make_task(tmp_path)
    task.create_job_script()
    job = Path(task.find_parameter(TAKLER_JOB).value)
    job.write_text("old content")
    job.chmod(mode)
    before = job.stat()

    task.create_job_script()

    after = job.stat()
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)
    assert stat.S_IMODE(after.st_mode) == mode | stat.S_IXUSR
    assert job.read_text() == "echo hello"
    assert list(job.parent.iterdir()) == [job]


def _default_acl(mask):
    # Linux POSIX ACL xattr encoding, used only by tests. Include named user
    # and group entries so chmod must preserve both entries and the mask.
    entries = [
        (1, 7, 0xFFFFFFFF),
        (2, 4, 65534),
        (4, 5, 0xFFFFFFFF),
        (8, 6, 65534),
        (16, mask, 0xFFFFFFFF),
        (32, 0, 0xFFFFFFFF),
    ]
    return struct.pack("<I", 2) + b"".join(
        struct.pack("<HHI", *entry) for entry in entries
    )


@pytest.mark.skipif(sys.platform != "linux", reason="Linux POSIX ACL encoding")
@pytest.mark.parametrize("with_umask", [0o022, 0o077, 0o002], indirect=True)
def test_default_acl_inheritance_and_overwrite(tmp_path, with_umask):
    task = _make_task(tmp_path)
    task.update_generated_parameters()
    job = Path(task.find_parameter(TAKLER_JOB).value)
    job.parent.mkdir(parents=True)
    os.setxattr(job.parent, "system.posix_acl_default", _default_acl(7))
    reference = job.parent / "reference"
    reference.write_text("")
    expected_acl = bytearray(os.getxattr(reference, "system.posix_acl_access"))
    owner = struct.unpack_from("<H", expected_acl, 6)[0]
    struct.pack_into("<H", expected_acl, 6, owner | 1)

    task.create_job_script()

    assert os.getxattr(job, "system.posix_acl_access") == bytes(expected_acl)
    assert stat.S_IMODE(job.stat().st_mode) == (
        stat.S_IMODE(reference.stat().st_mode) | stat.S_IXUSR
    )
    inode = job.stat().st_ino
    os.setxattr(job.parent, "system.posix_acl_default", _default_acl(4))
    task.create_job_script()
    assert job.stat().st_ino == inode
    assert os.getxattr(job, "system.posix_acl_access") == bytes(expected_acl)


def test_generation_uses_final_path_without_acl_queries(tmp_path, monkeypatch):
    task = _make_task(tmp_path)
    task.update_generated_parameters()
    job = Path(task.find_parameter(TAKLER_JOB).value)
    (tmp_path / "scripts" / "task1.takler").write_text(
        "#!/bin/sh\nprintf '你好\\n'\n", encoding="utf-8"
    )
    real_open = open
    calls = []

    def direct_open(path, mode, **kwargs):
        calls.append((Path(path), mode, kwargs))
        return real_open(path, mode, **kwargs)

    def unexpected(*args, **kwargs):
        pytest.fail("job generation must not query/copy ACLs or rename files")

    monkeypatch.setattr(shell_render, "open", direct_open, raising=False)
    for name in ("getxattr", "setxattr", "listxattr", "rename", "replace"):
        monkeypatch.setattr(os, name, unexpected, raising=False)
    task.create_job_script()
    assert calls == [(job, "w", {"encoding": "utf-8"})]
    assert list(job.parent.iterdir()) == [job]
    result = subprocess.run([str(job)], capture_output=True, check=True)
    assert result.stdout.decode("utf-8") == "你好\n"


@pytest.mark.parametrize("failure", ["render", "write", "chmod"])
def test_generation_failure_does_not_spawn(tmp_path, monkeypatch, failure):
    task = _make_task(tmp_path)
    task.create_job_script()
    job = Path(task.find_parameter(TAKLER_JOB).value)
    job.write_text("old content")
    spawn = Mock()
    monkeypatch.setattr(ShellRunner, "spawn", spawn)

    if failure == "render":
        (tmp_path / "scripts" / "task1.takler").write_text("{% if %}")
    elif failure == "write":
        real_open = open

        @contextlib.contextmanager
        def partial_write(path, mode, **kwargs):
            with real_open(path, mode, **kwargs) as stream:

                class FailingWriter:
                    def write(self, content):
                        stream.write(content[:4])
                        raise OSError("injected write failure")

                yield FailingWriter()

        monkeypatch.setattr(shell_render, "open", partial_write, raising=False)
    else:

        def fail_chmod(*args, **kwargs):
            raise OSError("injected chmod failure")

        monkeypatch.setattr(Path, "chmod", fail_chmod)

    with pytest.raises(JobSubmissionError, match="/flow1/task1"):
        task.submit()
    spawn.assert_not_called()
    assert task.do_run() is False
    assert task.state.node_status is NodeStatus.aborted
    spawn.assert_not_called()
    assert list(job.parent.iterdir()) == [job]
    if failure == "write":
        assert job.read_text() == "echo"


# ---------------------------------------------------------------------------
# Startup umask check (Requirement 12.8)
# ---------------------------------------------------------------------------


def _start_and_capture(auth_mode: str, monkeypatch, tmp_path) -> str:
    """Run ``TaklerServer.start()`` and return the captured log output.

    The three services and the snapshot restore are stubbed out: this test is
    about what ``start()`` logs before any of them runs, and binding a real port
    would make it an integration test. Log output is captured through a console
    sink configured inside the ``redirect_stderr`` block, because the logging
    backend does not route records into pytest's ``caplog`` handler.

    Auth_Mode reaches the server through ``TAKLER_AUTH_MODE``: it is resolved in
    ``TaklerServer.__init__`` from the environment and the Connect_Config, not
    from a constructor argument.

    A usable Operator_Secret_File is configured whatever the Auth_Mode is,
    because ``start()`` refuses to start an ``enabled`` server that has none
    (Requirement 7.3) and this test needs to reach the end of the start-up. The
    file is created with mode ``0o600`` so it does not add a permission WARNING
    of its own to the captured output.
    """
    monkeypatch.setenv("TAKLER_AUTH_MODE", auth_mode)

    secret_file = tmp_path / "operator.secret"
    secret_file.write_text("s3cret\n")
    secret_file.chmod(0o600)

    connect_config = generate_connect_config()
    connect_config.security.operator_secret_file = str(secret_file)

    async def _run() -> None:
        server = TaklerServer(host="login01", port=33083, connect_config=connect_config)

        async def _noop_async() -> None:
            return None

        server.scheduler.start = _noop_async
        server.grpc_transport.start = _noop_async
        server.checkpoint_manager.start = _noop_async
        server.checkpoint_manager.restore = lambda: None

        await server.start()

    buffer = io.StringIO()
    takler.logging._reset_configured_state()
    try:
        with contextlib.redirect_stderr(buffer):
            takler.logging.configure(level="DEBUG", console=True)
            asyncio.run(_run())
    finally:
        takler.logging.configure(console=True)
    return buffer.getvalue()


@pytest.mark.parametrize("with_umask", [0o022], indirect=True)
def test_startup_warns_exactly_once_when_auth_enabled_under_a_wide_umask(
    with_umask, monkeypatch, tmp_path
):
    output = _start_and_capture(
        auth_mode="enabled", monkeypatch=monkeypatch, tmp_path=tmp_path
    )

    assert output.count(_WARNING_MARKER) == 1
    assert "WARNING" in output
    # The line names the current umask, the risk and the recommended value.
    assert "0022" in output
    assert "TAKLER_PASS" in output
    assert "0077" in output


@pytest.mark.parametrize("with_umask", [0o077], indirect=True)
def test_startup_does_not_warn_when_auth_enabled_under_a_narrow_umask(
    with_umask, monkeypatch, tmp_path
):
    output = _start_and_capture(
        auth_mode="enabled", monkeypatch=monkeypatch, tmp_path=tmp_path
    )

    assert _WARNING_MARKER not in output


@pytest.mark.parametrize("with_umask", [0o022], indirect=True)
def test_startup_does_not_warn_when_auth_is_disabled(with_umask, monkeypatch, tmp_path):
    output = _start_and_capture(
        auth_mode="disabled", monkeypatch=monkeypatch, tmp_path=tmp_path
    )

    assert _WARNING_MARKER not in output
