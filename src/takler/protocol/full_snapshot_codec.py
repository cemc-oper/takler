"""Compact, lossless page rows for full snapshot v1.

Keys are defined once per version of this codec instead of once per node. The
public client expands rows before returning a structured node to its caller.
"""

from __future__ import annotations

SUMMARY_KEYS = (
    "name",
    "node_kind",
    "type_id",
    "status",
    "suspended",
    "definition_generation",
    "attempt_id",
)
DEFINITION_KEYS = (
    "trigger",
    "complete_trigger",
    "script_path",
    "times",
    "repeat",
    "events",
    "meters",
    "limits",
    "in_limits",
)
RUNTIME_KEYS = (
    "begun",
    "calendar",
    "task_id",
    "try_no",
    "attempt_id",
    "submitted_at",
    "started_at",
    "ended_at",
    "aborted_reason",
    "submission_unknown_reason",
    "events",
    "meters",
    "limits",
    "repeat",
    "status",
    "event_values",
    "meter_values",
    "limit_usage",
    "time_free",
    "repeat_value",
    "trigger_free",
    "complete_trigger_free",
)
ARTIFACT_KEYS = ("job_ref", "jobout_ref", "orvix_info_path", "not_applicable")
GROUP_KEYS = {
    "definition": DEFINITION_KEYS,
    "runtime": RUNTIME_KEYS,
    "artifacts": ARTIFACT_KEYS,
}


def pack_node(record: dict, field_groups: tuple[str, ...] | list[str]) -> list:
    path = record["path"]
    groups = []
    for group in field_groups:
        if group in ("summary", "service"):
            continue
        if group == "parameters":
            source = record["groups"]["parameters"]
            groups.append(
                [
                    [
                        [item["name"], item.get("value"), item["redacted"]]
                        for item in source[name]
                    ]
                    for name in ("user", "generated")
                ]
            )
        else:
            source = record["groups"][group]
            groups.append([source[key] for key in GROUP_KEYS[group]])
    row = [
        path,
        record["parent_path"],
        record["child_paths"],
        [record["summary"].get(key) for key in SUMMARY_KEYS],
        groups,
    ]
    if record.get("blob_refs"):
        row.append(record["blob_refs"])
    return row


def unpack_node(row: list, field_groups: tuple[str, ...] | list[str]) -> dict:
    if not isinstance(row, list) or len(row) not in (5, 6):
        raise ValueError("invalid compact full snapshot row")
    path, parent, children, summary, values = row[:5]
    if (
        not isinstance(path, str)
        or not isinstance(children, list)
        or not isinstance(values, list)
    ):
        raise ValueError("invalid compact full snapshot row")
    selected = [group for group in field_groups if group not in ("summary", "service")]
    if len(summary) != len(SUMMARY_KEYS) or len(values) != len(selected):
        raise ValueError("invalid compact full snapshot group count")
    groups = {}
    for group, value in zip(selected, values, strict=True):
        if group == "parameters":
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError("invalid compact parameters")
            groups[group] = {
                name: [
                    {
                        "name": item[0],
                        "source_path": path,
                        "value": item[1],
                        "redacted": item[2],
                    }
                    if not item[2]
                    else {"name": item[0], "source_path": path, "redacted": True}
                    for item in items
                ]
                for name, items in zip(("user", "generated"), value, strict=True)
            }
        else:
            keys = GROUP_KEYS[group]
            if not isinstance(value, list) or len(value) != len(keys):
                raise ValueError("invalid compact full snapshot group")
            groups[group] = dict(zip(keys, value, strict=True))
    result = {
        "path": path,
        "parent_path": parent,
        "child_paths": children,
        "summary": dict(zip(SUMMARY_KEYS, summary, strict=True)),
        "groups": groups,
    }
    if len(row) == 6:
        result["blob_refs"] = row[5]
    return result
