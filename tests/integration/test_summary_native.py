"""Existing tiny real-transport behaviors with proof of native decoding."""

import pytest

from takler.client import _summary_receiver
from tests.integration.test_query_client_a import (
    test_query_snapshot_detail_blob_and_show as test_query_snapshot_detail_blob_and_show,
)
from tests.integration.test_query_client_b import (
    test_query_since_sparse_and_structure_reset as test_query_since_sparse_and_structure_reset,
)

pytest.importorskip("takler_summary_kernel")


@pytest.fixture(autouse=True)
def require_native_path(monkeypatch):
    original = _summary_receiver._Receiver.decode
    counts = []

    def decode(self, raw):
        result = original(self, raw)
        counts.append(self.kernel.counts())
        return result

    monkeypatch.setattr(_summary_receiver._Receiver, "decode", decode)
    yield
    assert counts and any(native > 0 for native, _ in counts)
