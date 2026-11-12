"""Verify fixed summary encoding across full-size pages."""

from dataclasses import replace

import pytest

from takler.protocol.query_v1 import SummaryNode
from tests.query_fixtures import page_request
from tests.unit.server.test_summary_capture_encoding import (
    capture,
    changed,
    normalized,
    service,
)


@pytest.mark.parametrize("selection", [{}, {"scope_path": "/f"}, {"depth": 0}])
def test_encoded_pages_match_plain_rows_and_live_compatibility(selection):
    scheduler, query, flow = service(2300)
    flow.add_task('值"],"nodes":[]\\escape')
    try:
        root = scheduler.version_store.pin()
        plain = replace(
            root,
            nodes=tuple(
                tuple(replace(row, summary_encoding=None) for row in shard)
                for shard in root.nodes
            ),
        )
        assert normalized(capture(query, root, **selection)) == normalized(
            capture(query, plain, **selection)
        )
        legacy = query._capture(
            page_request(**selection), "owner", "query", "snapshot", 1, 32 * 1024**2
        )
        # Live projection time differs, but node fields and page splits agree.
        fixed_docs = normalized(capture(query, root, **selection))[1]
        live_docs = normalized(legacy)[1]
        assert [p["nodes"] for p in fixed_docs] == [p["nodes"] for p in live_docs]
        assert [p["flow_generations"] for p in fixed_docs] == [
            p["flow_generations"] for p in live_docs
        ]
    finally:
        query.close()


def test_controlled_capture_avoids_individual_summary_json_parses(monkeypatch):
    scheduler, query, _ = service(2300)
    root = scheduler.version_store.pin()
    calls = []
    original = SummaryNode.model_validate_json

    def parsed(raw, *args, **kwargs):
        calls.append(raw)
        return original(raw, *args, **kwargs)

    monkeypatch.setattr(SummaryNode, "model_validate_json", parsed)
    try:
        result = capture(query, root)
        assert len(result[1]) == 2 and not calls
        plain = changed(root, "/f/t0", summary_encoding=None)
        assert normalized(capture(query, plain)) == normalized(result)
        assert calls == [root.node("/f/t0").summary]
    finally:
        query.close()
