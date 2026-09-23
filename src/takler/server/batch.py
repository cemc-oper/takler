"""Ordered best-effort execution of control targets, without rollback."""

from functools import wraps
from copy import deepcopy

from takler.exceptions import InvalidNodePathError
from takler.protocol.commands import BatchItemResult, BatchResponse
from takler.protocol.error_code import error_code_for_exception, error_name_for_code


def _snapshot(scheduler):
    # Optional evidence for effect classification must never prevent execution.
    # An extension may not implement serialization; in that case failures have
    # unknown effects rather than blocking unrelated, valid control targets.
    try:
        return deepcopy(scheduler.bunch.to_dict())
    except Exception:
        return None


def batch_targets(field):
    """Adapt a single-target scheduler operation to the batch contract."""

    def decorate(operation):
        @wraps(operation)
        def execute(scheduler, command):
            begin = field == "flow_name"
            targets = (
                (
                    ["/" + command.flow_name]
                    if command.flow_name
                    else ["/" + name for name in scheduler.bunch.flows]
                )
                if begin
                else list(getattr(command, field))
            )
            if not targets and not begin:
                return BatchResponse(
                    flag=15, message="target list must not be empty", results=[]
                )
            results = []
            had_exception = False
            for index, target in enumerate(targets):
                before = None
                attempted = False
                try:
                    # Validate syntax per item, before the operation can mutate.
                    path, *attribute = target.split(":")
                    if (
                        not path.startswith("/")
                        or len(path) < 2
                        or any(part in ("", ".", "..") for part in path[1:].split("/"))
                        or len(attribute) > 1
                        or (
                            attribute
                            and (
                                not attribute[0]
                                or field != "paths"
                                or operation.__name__ != "run_command_force"
                            )
                        )
                        or (begin and "/" in path[1:])
                    ):
                        raise InvalidNodePathError(
                            "invalid target path", node_path=target
                        )
                    before = _snapshot(scheduler)
                    single = command.model_copy(
                        update={field: target[1:] if begin else [target]}
                    )
                    attempted = True
                    operation(scheduler, single)
                    effect = (
                        "none"
                        if before is not None and _snapshot(scheduler) == before
                        else "applied"
                    )
                    # Successful run represents acceptance even when a custom
                    # synchronous implementation leaves no serialized changes.
                    if operation.__name__ == "run_command_run":
                        effect = "applied"
                    item = BatchItemResult(
                        index=index,
                        target=target,
                        flag=0,
                        message="success",
                        effect=effect,
                    )
                except Exception as exc:
                    had_exception = True
                    flag = error_code_for_exception(exc)
                    effect = "unknown"
                    if not attempted:
                        effect = "none"
                    elif flag != 99 and before is not None:
                        after = _snapshot(scheduler)
                        if after is not None:
                            effect = "partial" if after != before else "none"
                    # Never expose arbitrary exception text or a job password.
                    item = BatchItemResult(
                        index=index,
                        target=target,
                        flag=flag,
                        message=error_name_for_code(flag),
                        effect=effect,
                    )
                results.append(item)
            failed = sum(item.flag != 0 for item in results)
            response = BatchResponse(
                flag=16 if failed else 0,
                message=f"processed={len(results)} succeeded={len(results) - failed} failed={failed}",
                results=results,
            )
            response._had_exception = had_exception
            return response

        return execute

    return decorate
