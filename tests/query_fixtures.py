"""Common query requests for snapshot tests."""

from takler.protocol.query_v1 import PageRequest


def page_request(**changes):
    data = dict(
        kind="page_request",
        schema_version=1,
        scope_path=None,
        flow_name=None,
        depth=None,
        field_groups=["summary"],
        cursor=None,
    )
    data.update(changes)
    return PageRequest(**data)
