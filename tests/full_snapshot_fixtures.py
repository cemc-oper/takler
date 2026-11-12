"""Small snapshots and in-process transport for query tests."""

import asyncio

from takler.core import Bunch, Flow
from takler.protocol.query_v1 import (
    CancelFullSnapshotRequest,
    FullSnapshotChunkRequest,
    FullSnapshotRequest,
)
from takler.server.scheduler import Scheduler


def _service(tasks=1, payload=""):
    bunch = Bunch("root")
    flow = bunch.add_flow(Flow("f"))
    for index in range(tasks):
        task = flow.add_task(f"t{index:04d}")
        if index == 0 and payload:
            task.add_parameter("PAYLOAD", payload)
    return Scheduler(bunch), flow


class _LocalTransport:
    def __init__(self, scheduler):
        self.service = scheduler.query_service
        self.mutate = lambda value: value

    def query(self, request):
        if isinstance(request, FullSnapshotRequest):
            result = asyncio.run(self.service.full_snapshots.page(request))
        elif isinstance(request, FullSnapshotChunkRequest):
            result = asyncio.run(self.service.full_snapshots.chunk(request))
        elif isinstance(request, CancelFullSnapshotRequest):
            result = asyncio.run(self.service.full_snapshots.cancel(request))
        else:
            result = self.service.capabilities()
        return self.mutate(result)
