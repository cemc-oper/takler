"""Lazy TUI projection of a compact QueryDocument snapshot."""

from __future__ import annotations

from takler.client.query_client import QuerySnapshot
from takler.query import NodeInfo, ParameterValue, ServiceInfo


def _parameters(items) -> dict[str, ParameterValue]:
    return {
        item.name: ParameterValue(
            "<redacted>" if item.redacted else item.value, item.redacted
        )
        for item in items
    }


class QueryTuiView:
    """Keep compact rows; materialize NodeInfo only for displayed or selected paths."""

    def __init__(self, snapshot: QuerySnapshot):
        self.query = snapshot
        self.name = snapshot.root_name
        self.service: ServiceInfo | None = None
        self.root_parameters: dict[str, ParameterValue] = {}
        self.server_generated: dict[str, ParameterValue] = {}
        self.children: dict[str, list[str]] = {}
        self.roots: list[str] = []
        self._nodes: dict[str, NodeInfo] = {}
        self._ancestors: dict[str, list[dict[str, ParameterValue]]] = {}
        for path in snapshot.ordered_paths:
            if path == snapshot.scope_path:
                if path != "/":
                    self.roots.append(path)
                continue
            parent = path.rsplit("/", 1)[0] or "/"
            self.children.setdefault(parent, []).append(path)
            if parent == "/":
                self.roots.append(path)

    def get(self, path: str) -> NodeInfo | None:
        row = self.query.get(path)
        if row is None:
            return None
        cached = self._nodes.get(path)
        if cached is not None:
            return cached
        parent = path.rsplit("/", 1)[0] or "/"
        node = NodeInfo(
            path.rsplit("/", 1)[-1],
            path,
            row.type_id,
            row.node_kind,
            row.status,
            row.suspended,
            path.count("/") - 1,
            None if parent == "/" else parent,
            children=self.children.get(path, []),
            attempt_id=row.attempt_id,
        )
        self._nodes[path] = node
        return node

    def apply_detail(self, path: str, reply) -> NodeInfo | None:
        if path == "/":
            self.apply_root_detail(reply)
        node = self.get(path)
        if node is None:
            return None
        groups = reply.groups
        if groups.parameters is not None:
            node.parameters = _parameters(groups.parameters.user)
            self._ancestors[path] = [
                _parameters(source.user) for source in groups.parameters.ancestors
            ]
        if groups.definition is not None:
            definition = groups.definition
            node.trigger = definition.trigger
            node.complete_trigger = definition.complete_trigger
            node.script_path = definition.script_path
            node.times = [str(item.get("time", "")) for item in definition.times]
            node.in_limits = [
                (
                    item.get("limit_name", ""),
                    item.get("tokens", 1),
                    item.get("node_path"),
                )
                for item in definition.in_limits
            ]
            node.limits = [
                (item.get("name", ""), str(item.get("limit", "")))
                for item in definition.limits
            ]
            if definition.repeat:
                repeat = definition.repeat
                node.repeat = (
                    f"{repeat.get('name', '')} "
                    f"[{repeat.get('start_date', '')}, {repeat.get('end_date', '')}]"
                )
        if groups.runtime is not None:
            runtime = groups.runtime
            for key in (
                "task_id",
                "try_no",
                "submitted_at",
                "started_at",
                "ended_at",
                "aborted_reason",
                "attempt_id",
            ):
                setattr(node, key, getattr(runtime, key))
            node.events = [
                (item.get("name", ""), "set" if item.get("value") else "unset")
                for item in runtime.events
            ]
            meters = (
                {item.get("name"): item for item in groups.definition.meters}
                if groups.definition is not None
                else {}
            )
            node.meters = [
                (
                    item.get("name", ""),
                    str(meters.get(item.get("name"), {}).get("min_value", "")),
                    str(meters.get(item.get("name"), {}).get("max_value", "")),
                    str(item.get("value", "")),
                )
                for item in runtime.meters
            ]
            limits = (
                {
                    item.get("name"): item.get("limit", "")
                    for item in groups.definition.limits
                }
                if groups.definition is not None
                else {}
            )
            node.limits = [
                (
                    item.get("name", ""),
                    f"{item.get('value', 0)}/{limits.get(item.get('name'), '')}",
                )
                for item in runtime.limits
            ]
            if runtime.repeat and node.repeat:
                node.repeat = node.repeat.replace(
                    " [", f" {runtime.repeat.get('value', '')} [", 1
                )
        if groups.artifacts is not None:
            artifacts = groups.artifacts
            node.job_path = artifacts.job_ref.path if artifacts.job_ref else None
            node.jobout_path = (
                artifacts.jobout_ref.path if artifacts.jobout_ref else None
            )
            node.orvix_info_path = artifacts.orvix_info_path
            if artifacts.job_ref:
                node.generated["TAKLER_JOB"] = ParameterValue(artifacts.job_ref.path)
            if artifacts.jobout_ref:
                node.generated["TAKLER_JOBOUT"] = ParameterValue(
                    artifacts.jobout_ref.path
                )
        return node

    def apply_root_detail(self, reply) -> None:
        groups = reply.groups
        if groups.parameters is not None:
            self.root_parameters = _parameters(groups.parameters.user)
        if groups.service is not None:
            service = groups.service
            self.service = ServiceInfo(
                service.status,
                service.status_reason,
                tuple(service.halt_causes),
                service.last_checkpoint_at,
                service.restore_summary,
                service.message or "",
            )
            self.server_generated = _parameters(service.generated_defaults)

    def inherited_parameters(self, path: str) -> dict[str, str]:
        node = self.get(path)
        if node is None:
            return {}
        seen = set(node.parameters) | set(node.generated)
        result: dict[str, str] = {}
        for source in [
            *self._ancestors.get(path, []),
            self.root_parameters,
            self.server_generated,
        ]:
            for name, value in source.items():
                if name not in seen:
                    result[name] = value.text
                    seen.add(name)
        return result

    def resolve_parameter(self, path: str, name: str) -> ParameterValue | None:
        node = self.get(path)
        if node is None:
            return None
        for source in [
            node.parameters,
            node.generated,
            *self._ancestors.get(path, []),
            self.root_parameters,
            self.server_generated,
        ]:
            if name in source:
                return source[name]
        return None

    def lookup_parameter(self, path: str, name: str) -> str | None:
        value = self.resolve_parameter(path, name)
        return (
            None
            if value is None or value.redacted or value.value is None
            else value.text
        )
