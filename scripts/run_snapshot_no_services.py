"""Pytest plugin fencing the selected G0 suites off from service startup."""

import pytest


@pytest.fixture(autouse=True)
def forbid_services(monkeypatch):
    from takler.server import TaklerServer
    from takler.server.grpc_transport import GrpcTransport
    from takler.server.checkpoint import CheckpointManager
    from takler.server.scheduler import Scheduler

    async def forbidden(*args, **kwargs):
        raise AssertionError("G0 forbids network/scheduler/checkpoint service startup")

    for owner in (TaklerServer, GrpcTransport, CheckpointManager, Scheduler):
        monkeypatch.setattr(owner, "start", forbidden)
