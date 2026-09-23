"""Explicit registrations installed by trusted startup code, never by documents."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Callable, Literal, ForwardRef

from pydantic import BaseModel, Field, create_model

from takler.schema.definition import DefinitionError


@dataclass(frozen=True)
class NodeRegistration:
    type_id: str
    python_type: type
    kind: Literal["bunch", "flow", "container", "task"]
    definition_schema: type[BaseModel] | None = None
    export_definition: Callable | None = None
    construct: Callable | None = None
    runtime_schema: type[BaseModel] | None = None
    export_runtime: Callable | None = None
    restore_runtime: Callable | None = None
    query: Callable | None = None
    secret_fields: frozenset[str] = frozenset()


class TypeRegistry:
    def __init__(self):
        self._ids = {}
        self._types = {}
        self._document_model = None

    def register(self, entry: NodeRegistration, *, _builtin=False):
        from takler.core import Bunch, Flow, NodeContainer, Task

        expected = {
            "bunch": Bunch,
            "flow": Flow,
            "container": NodeContainer,
            "task": Task,
        }
        if (
            entry.type_id in self._ids
            or entry.python_type in self._types
            or not entry.type_id
            or "." not in entry.type_id
            or (entry.type_id.startswith("takler.") and not _builtin)
            or entry.kind not in expected
            or not issubclass(entry.python_type, expected[entry.kind])
        ):
            raise DefinitionError("invalid_registration")
        actual_kind = next(
            kind
            for kind in ("bunch", "flow", "container", "task")
            if issubclass(entry.python_type, expected[kind])
        )
        if actual_kind != entry.kind:
            raise DefinitionError("invalid_registration")
        for schema in (entry.definition_schema, entry.runtime_schema):
            if schema is not None and (
                schema.model_config.get("extra") != "forbid"
                or not schema.model_config.get("strict")
            ):
                raise DefinitionError("invalid_registration")
        if entry.definition_schema is not None:
            forbidden = {
                "job_password",
                "task_id",
                "try_no",
                "state",
                "suspended",
                "auth",
                "secret",
                "TAKLER_PASS",
                "TAKLER_SECRET",
            } | set(entry.secret_fields)
            if forbidden.intersection(entry.definition_schema.model_fields):
                raise DefinitionError("invalid_registration")
        self._ids[entry.type_id] = entry
        self._types[entry.python_type] = entry
        self._document_model = None

    def by_id(self, type_id):
        if not isinstance(type_id, str) or type_id not in self._ids:
            raise DefinitionError("unknown_type")
        return self._ids[type_id]

    def by_type(self, python_type):
        if python_type not in self._types:
            raise DefinitionError("unregistered_type")
        return self._types[python_type]

    def document_model(self):
        """Create a data-only schema from trusted registration descriptors."""
        if self._document_model is not None:
            return self._document_model
        from functools import reduce
        from operator import or_
        from typing import Annotated
        from takler.schema.definition import (
            BunchDefinition,
            FlowDefinition,
            ContainerDefinition,
            TaskDefinition,
            ShellDefinition,
            DefinitionDocument,
        )

        if all(key.startswith("takler.") for key in self._ids):
            self._document_model = DefinitionDocument
            return self._document_model
        bases = {
            "bunch": BunchDefinition,
            "flow": FlowDefinition,
            "container": ContainerDefinition,
            "task": TaskDefinition,
        }
        models = []
        for index, entry in enumerate(self._ids.values()):
            fields = {"type_id": (Literal[entry.type_id], ...)}
            if entry.kind == "bunch":
                fields["flows"] = (
                    list[ForwardRef("RegisteredFlow")],
                    Field(default_factory=list),
                )
            else:
                fields["children"] = (
                    list[ForwardRef("RegisteredChild")],
                    Field(default_factory=list),
                )
            base = (
                ShellDefinition
                if entry.type_id == "takler.shell"
                else bases[entry.kind]
            )
            if not entry.type_id.startswith("takler."):
                if entry.definition_schema is None:
                    continue  # Missing codec is diagnosed before validation/build.
                fields["type_data"] = (entry.definition_schema, ...)
            models.append(
                (
                    entry.kind,
                    create_model(f"RegisteredNode{index}", __base__=base, **fields),
                )
            )

        def union(kinds):
            return Annotated[
                reduce(or_, [model for kind, model in models if kind in kinds]),
                Field(discriminator="type_id"),
            ]

        namespace = {
            "RegisteredChild": union({"container", "task"}),
            "RegisteredFlow": union({"flow"}),
        }
        for _, model in models:
            model.model_rebuild(_types_namespace=namespace)
        self._document_model = create_model(
            "RegisteredDefinitionDocument",
            __base__=DefinitionDocument,
            root=(union({"bunch", "flow"}), ...),
        )
        return self._document_model


_current = ContextVar("takler_type_registry", default=None)
_default = None


def get_registry():
    global _default
    registry = _current.get()
    if registry is not None:
        return registry
    if _default is None:
        _default = builtin_registry()
    return _default


@contextmanager
def using_registry(registry):
    token = _current.set(registry)
    try:
        yield registry
    finally:
        _current.reset(token)


def builtin_registry():
    from takler.core import Bunch, Flow, NodeContainer, Task
    from takler.tasks.shell import ShellScriptTask

    registry = TypeRegistry()
    for cls, type_id, kind in (
        (Bunch, "takler.bunch", "bunch"),
        (Flow, "takler.flow", "flow"),
        (NodeContainer, "takler.container", "container"),
        (Task, "takler.task", "task"),
        (ShellScriptTask, "takler.shell", "task"),
    ):
        registry.register(NodeRegistration(type_id, cls, kind), _builtin=True)
    return registry


def require_codec(entry, *names):
    if not entry.type_id.startswith("takler.") and any(
        getattr(entry, name) is None for name in names
    ):
        raise DefinitionError("missing_codec")


def safe_codec(function):
    """Keep user values out of diagnostics from trusted extension codecs."""
    from functools import wraps

    @wraps(function)
    def call(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except DefinitionError:
            raise
        except Exception:
            raise DefinitionError("invalid_field") from None

    return call


def validate_definition_data(data, secret_fields=()):
    """Extension data cannot smuggle runtime/credential fields into definitions."""
    forbidden = {
        "job_password",
        "task_id",
        "try_no",
        "state",
        "suspended",
        "auth",
        "secret",
        "takler_pass",
        "takler_secret",
    }
    forbidden.update(name.lower() for name in secret_fields)

    def check(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str) or key.lower() in forbidden:
                    raise DefinitionError("forbidden_field")
                check(item)
        elif isinstance(value, list):
            for item in value:
                check(item)
        elif value is not None and type(value) not in (str, int, float, bool):
            raise DefinitionError("invalid_field")
        elif type(value) is float:
            from math import isfinite

            if not isfinite(value):
                raise DefinitionError("invalid_field")

    check(data)


@safe_codec
def export_definition_data(entry, node):
    require_codec(entry, "definition_schema", "export_definition")
    data = entry.definition_schema.model_validate(
        entry.export_definition(node)
    ).model_dump(mode="json")
    validate_definition_data(data, entry.secret_fields)
    return data
