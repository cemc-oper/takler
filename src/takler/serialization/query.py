"""Allowlisted, read-only projection of live scheduler state for show."""

from pathlib import Path

from takler.core import Bunch, Flow, Task
from takler.core.calendar import Calendar
from takler.query import REDACTED, SENSITIVE_PARAMETERS
from takler.schema import DefinitionError
from takler.serialization.registry import get_registry
from takler.tasks.shell.shell_script_task import ShellScriptTask


def project_show(bunch, redacted_parameters=()):
    hidden = SENSITIVE_PARAMETERS | {name.lower() for name in redacted_parameters}

    def project(node):
        # Registrations supply labels only. Never invoke definition/runtime
        # exporters or constructors, which may expose private plugin fields.
        try:
            entry = get_registry().by_type(type(node))
            kind, type_id = entry.kind, entry.type_id
        except DefinitionError:
            kind = (
                "bunch"
                if isinstance(node, Bunch)
                else "flow"
                if isinstance(node, Flow)
                else "task"
                if isinstance(node, Task)
                else "container"
            )
            type_id = type(node).__name__
        result = {
            "name": node.name,
            "type_id": type_id,
            "node_kind": kind,
            "state": {
                "status": node.state.node_status.value,
                "suspended": node.state.suspended,
            },
        }
        redacted = set()

        def parameters(values):
            out = []
            for name, value in values.items():
                if name.lower() in hidden:
                    value = REDACTED
                    redacted.add(name)
                elif isinstance(value, Path):
                    value = str(value)
                out.append({"name": name, "value": value})
            return out

        result["user_parameters"] = parameters(
            {k: p.value for k, p in node.user_parameters.items()}
        )
        generated = {}
        if isinstance(node, Bunch):
            generated.update(
                {p.name: p.value for p in node.server_state.server_parameters}
            )
        if node.repeat is not None:
            r = node.repeat.r
            generated[r.name] = r.value
            result["repeat"] = {
                "r": {
                    "name": r.name,
                    "value": REDACTED if r.name.lower() in hidden else r.value,
                    "start_date": str(r.start_date),
                    "end_date": str(r.end_date),
                    "step": r.step,
                }
            }
        if isinstance(node, Flow):
            result["begun"] = node.begun
            result["calendar"] = Calendar.to_dict(node.calendar)
            generated.update(
                DATE=node.calendar.flow_time.strftime("%Y-%m-%d")
                if node.calendar.flow_time
                else None,
                TIME=node.calendar.flow_time.strftime("%H:%M")
                if node.calendar.flow_time
                else None,
            )
        if isinstance(node, Task):
            result.update(
                task_id=node.task_id,
                try_no=node.try_no,
                aborted_reason=node.aborted_reason.replace(node.job_password, REDACTED)
                if node.aborted_reason and node.job_password
                else node.aborted_reason,
            )
            generated.update(
                TASK=node.name,
                TAKLER_NAME=node.node_path,
                TAKLER_RID=node.task_id,
                TAKLER_TRY_NO=node.try_no,
            )
        if isinstance(node, ShellScriptTask):
            result["script_path"] = (
                str(node.script_path) if node.script_path is not None else None
            )
            if "takler_script" in hidden:
                result["script_path"] = REDACTED
            generated["TAKLER_SCRIPT"] = result["script_path"]
            home = node.find_parent_parameter("TAKLER_HOME")
            if "takler_home" in hidden:
                generated.update(TAKLER_JOB=REDACTED, TAKLER_JOBOUT=REDACTED)
                redacted.update(("TAKLER_JOB", "TAKLER_JOBOUT"))
            elif home is not None and home.value is not None:
                generated["TAKLER_JOB"] = str(
                    Path(f"{home.value}{node.node_path}.job{node.try_no}").absolute()
                )
                generated["TAKLER_JOBOUT"] = str(
                    Path(f"{home.value}{node.node_path}.{node.try_no}").absolute()
                )
        result["generated_parameters"] = parameters(generated)
        result["redacted_parameters"] = sorted(redacted)
        for key, expression in (
            ("trigger", node.trigger_expression),
            ("complete_trigger", node.complete_trigger_expression),
        ):
            if expression is not None:
                result[key] = expression.expression_str
        result["events"] = [{"name": e.name, "value": e.value} for e in node.events]
        result["meters"] = [
            {
                "name": m.name,
                "min_value": m.min_value,
                "max_value": m.max_value,
                "value": m.value,
            }
            for m in node.meters
        ]
        result["limits"] = [
            {"name": limit.name, "value": limit.value, "limit": limit.limit}
            for limit in node.limits
        ]
        result["in_limit_manager"] = {
            "in_limit_list": [
                {
                    "limit_name": limit.limit_name,
                    "tokens": limit.tokens,
                    "node_path": limit.node_path,
                }
                for limit in node.in_limit_manager.in_limit_list
            ]
        }
        result["times"] = [{"time": t.time.strftime("%H:%M")} for t in node.times]
        if isinstance(node, Bunch):
            result["flows"] = [project(flow) for flow in node.flows.values()]
        else:
            result["children"] = [project(child) for child in node.children]
        return result

    return project(bunch)
