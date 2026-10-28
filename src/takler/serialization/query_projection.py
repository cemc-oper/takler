"""Bounded-scope, read-only projections for QueryDocument v1.

The caller owns snapshot capture, identity checks, page budgets, and transport.
This module reads one live state point; it never runs execution serializers or
materializes an unrequested detail group.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path

from takler.core import Bunch, Flow, Task
from takler.core.node import Node
from takler.core.repeat import RepeatDate
from takler.protocol.query_v1 import (
    DETAIL_GROUPS,
    DetailGroups,
    SummaryNode,
    valid_path,
)
from takler.query import REDACTED
from takler.serialization.query import query_hidden_parameters, query_node_type
from takler.tasks.shell.shell_script_task import ShellScriptTask


def find_query_node(bunch: Bunch, path: str) -> Node | None:
    """Resolve a canonical query path, including the root Bunch."""
    valid_path(path)
    return bunch if path == "/" else bunch.find_node(path)


def iter_query_summaries(
    bunch: Bunch,
    *,
    scope_path: str = "/",
    flow_name: str | None = None,
    depth: int | None = None,
) -> Iterator[tuple[SummaryNode, str | None]]:
    """Yield preorder summaries and their Flow generation without recursion.

    Depth is relative to the requested root. Iteration is lazy so the caller
    can stop at a page boundary without projecting the rest of a large tree.
    A snapshot owner must capture the yielded values before live state changes.
    """
    if depth is not None and (type(depth) is not int or depth < 0):
        raise ValueError("invalid depth")
    if flow_name is not None:
        if scope_path != "/":
            raise ValueError("scope and flow are mutually exclusive")
        if not flow_name:
            raise ValueError("invalid flow name")
        valid_path("/" + flow_name)
        if "/" in flow_name:
            raise ValueError("invalid flow name")
        scope_path = "/" + flow_name
    start = find_query_node(bunch, scope_path)
    if start is None:
        raise ValueError("scope not found")
    flow = None if scope_path == "/" else bunch.find_flow(scope_path.split("/", 2)[1])
    generation = None if flow is None else flow.definition_generation
    pending = [(start, scope_path, 0, generation)]
    while pending:
        node, path, level, current_generation = pending.pop()
        kind, type_id = query_node_type(node)
        summary_fields = dict(
            path=path,
            node_kind=kind,
            type_id=type_id,
            status=node.state.node_status.name,
            suspended=node.state.suspended,
        )
        if isinstance(node, Task) and node.attempt_id is not None:
            summary_fields["attempt_id"] = node.attempt_id
            if node.submission_unknown_reason is not None:
                summary_fields["submission_unknown"] = True
        yield SummaryNode(**summary_fields), current_generation
        if depth is not None and level >= depth:
            continue
        children = node.flows.values() if isinstance(node, Bunch) else node.children
        for child in reversed(tuple(children)):
            child_path = "/" + child.name if path == "/" else path + "/" + child.name
            child_generation = (
                child.definition_generation
                if isinstance(child, Flow)
                else current_generation
            )
            pending.append((child, child_path, level + 1, child_generation))


def _parameter_items(parameters, hidden: frozenset[str] | set[str]) -> list[dict]:
    result = []
    for name, parameter in parameters.items():
        if name.lower() in hidden:
            result.append({"name": name, "redacted": True})
            continue
        value = parameter.value
        if isinstance(value, Path):
            value = str(value)
        if value is not None and type(value) not in (str, int, float, bool):
            raise ValueError("unsupported public parameter value")
        if type(value) is float and not isfinite(value):
            raise ValueError("non-finite public parameter value")
        result.append({"name": name, "value": value})
    return result


def _parameters(node: Node, bunch: Bunch, path: str, hidden) -> dict:
    ancestors = []
    parent = node.parent
    parent_path = path
    while parent is not None:
        parent_path = parent_path.rsplit("/", 1)[0] or "/"
        ancestors.append(
            {
                "source_path": parent_path,
                "user": _parameter_items(parent.user_parameters, hidden),
            }
        )
        parent = parent.parent
    if node is not bunch:
        ancestors.append(
            {
                "source_path": "/",
                "user": _parameter_items(bunch.user_parameters, hidden),
            }
        )
    return {
        "user": _parameter_items(node.user_parameters, hidden),
        "ancestors": ancestors,
    }


def _definition(node: Node, hidden) -> dict:
    repeat = None
    if node.repeat is not None and node.repeat.r is not None:
        r = node.repeat.r
        if isinstance(r, RepeatDate):
            repeat = {
                "name": r.name,
                "start_date": r.start_date.isoformat(),
                "end_date": r.end_date.isoformat(),
                "step": r.step,
            }
    script_path = None
    if isinstance(node, ShellScriptTask) and node.script_path is not None:
        script_path = REDACTED if "takler_script" in hidden else str(node.script_path)
    return {
        "trigger": node.trigger_expression.expression_str
        if node.trigger_expression
        else None,
        "complete_trigger": (
            node.complete_trigger_expression.expression_str
            if node.complete_trigger_expression
            else None
        ),
        "script_path": script_path,
        "times": [{"time": item.time.strftime("%H:%M")} for item in node.times],
        "repeat": repeat,
        "events": [
            {"name": item.name, "initial_value": item.initial_value}
            for item in node.events
        ],
        "meters": [
            {
                "name": item.name,
                "min_value": item.min_value,
                "max_value": item.max_value,
            }
            for item in node.meters
        ],
        "limits": [{"name": item.name, "limit": item.limit} for item in node.limits],
        "in_limits": [
            {
                "limit_name": item.limit_name,
                "tokens": item.tokens,
                "node_path": item.node_path,
            }
            for item in node.in_limit_manager.in_limit_list
        ],
    }


def _utc_observation(value: str | None) -> str | None:
    if value is None:
        return None
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("task observation time has no timezone")
    return instant.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _runtime(node: Node, hidden) -> dict:
    result = {
        "events": [{"name": item.name, "value": item.value} for item in node.events],
        "meters": [{"name": item.name, "value": item.value} for item in node.meters],
        "limits": [{"name": item.name, "value": item.value} for item in node.limits],
        "repeat": None,
    }
    if node.repeat is not None and node.repeat.r is not None:
        r = node.repeat.r
        result["repeat"] = {
            "name": r.name,
            "value": REDACTED if r.name.lower() in hidden else r.value,
        }
    if isinstance(node, Flow):
        result["begun"] = node.begun
        result["calendar_flow_time"] = (
            node.calendar.flow_time.isoformat()
            if node.calendar.flow_time is not None
            else None
        )
    if isinstance(node, Task):
        result.update(
            attempt_id=node.attempt_id,
            try_no=node.try_no,
            task_id=node.task_id,
            submitted_at=_utc_observation(node.submitted_at),
            started_at=_utc_observation(node.started_at),
            ended_at=_utc_observation(node.ended_at),
            aborted_reason=(
                node.aborted_reason.replace(node.job_password, REDACTED)
                if node.aborted_reason and node.job_password
                else node.aborted_reason
            ),
            submission_unknown_reason=(
                node.submission_unknown_reason.replace(node.job_password, REDACTED)
                if node.submission_unknown_reason and node.job_password
                else node.submission_unknown_reason
            ),
        )
    return result


def _artifacts(node: Node, hidden) -> dict:
    result = {"job_ref": None, "jobout_ref": None, "orvix_info_path": None}
    if not isinstance(node, Task) or "takler_home" in hidden:
        return result
    current = node.attempt_id is not None
    home = None
    if isinstance(node, ShellScriptTask):
        home_parameter = node.find_parent_parameter("TAKLER_HOME")
        if home_parameter is not None and home_parameter.value is not None:
            home = home_parameter.value
    suffix = node.attempt_id or str(node.try_no)
    for field, raw, name, extension in (
        ("job_ref", node.job_path, "takler_job", ".job"),
        ("jobout_ref", node.jobout_path, "takler_jobout", "."),
    ):
        if name in hidden:
            continue
        if current and raw is not None:
            result[field] = {"path": raw, "origin": "current"}
        elif home is not None:
            path = Path(f"{home}{node.node_path}{extension}{suffix}").absolute()
            result[field] = {"path": str(path), "origin": "anticipated"}
    if current and "takler_job" not in hidden:
        result["orvix_info_path"] = node.orvix_info_path
    return result


def _service(bunch: Bunch, hidden, server_status: dict | None) -> dict:
    status = server_status or bunch.server_state.to_dict()
    return {
        "status": status["status"],
        "status_reason": status.get("status_reason"),
        "halt_causes": status["halt_causes"],
        "message": status.get("message"),
        "last_checkpoint_at": status.get("last_checkpoint_at"),
        "restore_summary": status.get("restore_summary"),
        "generated_defaults": _parameter_items(
            bunch.server_config.generated_parameters(), hidden
        ),
    }


def project_query_detail(
    bunch: Bunch,
    path: str,
    field_groups: Sequence[str],
    *,
    redacted_parameters: Sequence[str] = (),
    server_status: dict | None = None,
) -> DetailGroups:
    """Project requested typed groups only; leave identity and as_of to R2-07A."""
    if (
        not field_groups
        or len(set(field_groups)) != len(field_groups)
        or set(field_groups) - DETAIL_GROUPS
    ):
        raise ValueError("invalid detail groups")
    node = find_query_node(bunch, path)
    if node is None:
        raise ValueError("node not found")
    if "service" in field_groups and node is not bunch:
        raise ValueError("service detail is only available for root")
    hidden = query_hidden_parameters(redacted_parameters)
    groups = {}
    for group in field_groups:
        if group == "parameters":
            groups[group] = _parameters(node, bunch, path, hidden)
        elif group == "definition":
            groups[group] = _definition(node, hidden)
        elif group == "runtime":
            groups[group] = _runtime(node, hidden)
        elif group == "artifacts":
            groups[group] = _artifacts(node, hidden)
        elif group == "service":
            groups[group] = _service(bunch, hidden, server_status)
    return DetailGroups.model_validate(groups)
