"""Version 1 workflow definitions. This module must not import execution code."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from pydantic_core import PydanticCustomError


class DefinitionError(ValueError):
    """A safe diagnostic containing a rule and location, never input values."""

    def __init__(self, code: str, document_path: str = "$"):
        self.code = code
        self.document_path = document_path
        super().__init__(f"{code} at {document_path}")


class DefinitionModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, allow_inf_nan=False, hide_input_in_errors=True
    )


Name = Annotated[str, Field(min_length=1)]


class ParameterDefinition(DefinitionModel):
    name: Name
    value: str | int | float | bool | None

    @model_validator(mode="after")
    def allowed_name(self):
        if self.name.upper() in {
            "TAKLER_PASS",
            "TAKLER_SECRET",
            "TAKLER_HOST",
            "TAKLER_PORT",
        }:
            raise PydanticCustomError("forbidden_field", "forbidden parameter")
        return self


class EventDefinition(DefinitionModel):
    name: Name
    initial_value: bool = False


class MeterDefinition(DefinitionModel):
    name: Name
    min_value: int
    max_value: int

    @model_validator(mode="after")
    def bounds(self):
        if self.min_value > self.max_value:
            raise ValueError("invalid meter bounds")
        return self


class LimitDefinition(DefinitionModel):
    name: Name
    limit: Annotated[int, Field(ge=0)]


class InLimitDefinition(DefinitionModel):
    limit_name: Name
    node_path: str | None = None
    tokens: Annotated[int, Field(gt=0)] = 1


class RepeatDateDefinition(DefinitionModel):
    type_id: Literal["takler.repeat.date"]
    name: Name
    start_date: str
    end_date: str
    step: Annotated[int, Field(gt=0)] = 1

    @model_validator(mode="after")
    def dates(self):
        for value in (self.start_date, self.end_date):
            if not re.fullmatch(r"[0-9]{8}", value):
                raise ValueError("invalid date")
            datetime.strptime(value, "%Y%m%d")
        if self.start_date > self.end_date:
            raise ValueError("invalid date range")
        return self


class TimeDefinition(DefinitionModel):
    time: Annotated[str, Field(pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")]


def unique(values):
    if len(values) != len(set(values)):
        raise PydanticCustomError("duplicate_name", "duplicate definition item")


class NodeDefinition(DefinitionModel):
    name: Name
    user_parameters: list[ParameterDefinition] = Field(default_factory=list)
    default_node_status: Literal["queued", "complete"] = "queued"
    trigger: Name | None = None
    complete_trigger: Name | None = None
    events: list[EventDefinition] = Field(default_factory=list)
    meters: list[MeterDefinition] = Field(default_factory=list)
    limits: list[LimitDefinition] = Field(default_factory=list)
    in_limits: list[InLimitDefinition] = Field(default_factory=list)
    repeat: RepeatDateDefinition | None = None
    times: list[TimeDefinition] = Field(default_factory=list)
    # Builtins have no extra definition data. R0-11 supplies registered schemas.
    type_data: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def invariants(self):
        if self.name in {".", ".."} or any(c in self.name for c in "/:\0"):
            raise ValueError("invalid node name")
        if self.type_data:
            raise ValueError("builtin type_data must be empty")
        for values in (self.user_parameters, self.events, self.meters, self.limits):
            unique([item.name for item in values])
        unique([(item.node_path, item.limit_name) for item in self.in_limits])
        unique([item.time for item in self.times])
        return self


class TaskDefinition(NodeDefinition):
    type_id: Literal["takler.task"]
    children: list[ChildDefinition] = Field(default_factory=list)

    @model_validator(mode="after")
    def leaf(self):
        if self.children:
            raise PydanticCustomError("invalid_structure", "task must be a leaf")
        return self


class ShellDefinition(TaskDefinition):
    type_id: Literal["takler.shell"]
    script_path: str | None = None


class ContainerDefinition(NodeDefinition):
    type_id: Literal["takler.container"]
    children: list[ChildDefinition] = Field(default_factory=list)

    @model_validator(mode="after")
    def siblings(self):
        unique([child.name for child in self.children])
        return self


class FlowDefinition(ContainerDefinition):
    type_id: Literal["takler.flow"]


ChildDefinition = Annotated[
    TaskDefinition | ShellDefinition | ContainerDefinition,
    Field(discriminator="type_id"),
]


class BunchDefinition(DefinitionModel):
    type_id: Literal["takler.bunch"]
    name: str
    user_parameters: list[ParameterDefinition] = Field(default_factory=list)
    flows: list[FlowDefinition] = Field(default_factory=list)

    @model_validator(mode="after")
    def invariants(self):
        if self.name in {".", ".."} or any(c in self.name for c in "/:\0"):
            raise ValueError("invalid bunch name")
        unique([item.name for item in self.user_parameters])
        unique([flow.name for flow in self.flows])
        return self


class DefinitionDocument(DefinitionModel):
    kind: Literal["takler.definition"]
    schema_version: int
    root: Annotated[BunchDefinition | FlowDefinition, Field(discriminator="type_id")]

    @model_validator(mode="before")
    @classmethod
    def version(cls, value):
        if isinstance(value, dict) and "schema_version" in value:
            version = value["schema_version"]
            if type(version) is not int or version != 1:
                raise PydanticCustomError(
                    "unsupported_version", "unsupported definition version"
                )
        return value

    @classmethod
    def model_validate_json(cls, json_data, **kwargs):
        # Pydantic's JSON decoder accepts duplicate keys; use the strict boundary.
        return cls.model_validate(_decode_json(json_data), **kwargs)


for _model in (TaskDefinition, ShellDefinition, ContainerDefinition, FlowDefinition):
    _model.model_rebuild()


def _decode_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise DefinitionError("invalid_document")
            result[key] = value
        return result

    def constant(_):
        raise DefinitionError("invalid_document")

    try:
        return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, TypeError, UnicodeError):
        raise DefinitionError("invalid_document") from None


def parse_definition(data: str | bytes | dict) -> DefinitionDocument:
    """Validate a definition with diagnostics safe for logs and wire responses.

    Expression syntax and resolved references are checked by the builder, not
    by this execution-independent field model.
    """
    try:
        if isinstance(data, (str, bytes)):
            data = _decode_json(data)
        return DefinitionDocument.model_validate(data)
    except ValidationError as exc:
        error = exc.errors(include_input=False, include_context=False)[0]
        code = error["type"]
        if code == "extra_forbidden" and error["loc"][-1] in {
            "state",
            "status",
            "suspended",
            "task_id",
            "try_no",
            "job_password",
            "begun",
            "calendar",
            "trigger_free",
            "complete_trigger_free",
            "is_complete_triggered",
            "parent",
            "node_path",
            "class_type",
            "server_state",
            "server_parameters",
            "generated_parameters",
            "auth",
            "secret",
            "value",
            "free",
            "node_paths",
        }:
            code = "forbidden_field"
        if code not in {
            "unsupported_version",
            "forbidden_field",
            "duplicate_name",
            "invalid_structure",
        }:
            code = "unknown_type" if code == "union_tag_invalid" else "invalid_field"
        # Extra keys are untrusted text too; do not echo them in diagnostics.
        path = (
            "$"
            if error["type"] == "extra_forbidden"
            else "$." + ".".join(map(str, error["loc"]))
        )
        raise DefinitionError(code, path) from None
