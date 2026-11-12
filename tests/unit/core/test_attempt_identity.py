"""Current execution identity is independent of the resettable try counter."""

from datetime import datetime
from uuid import UUID

from takler.core import Bunch, Task
from takler.core.parameter import TAKLER_ATTEMPT_ID, TAKLER_PASS, TAKLER_RID
from takler.core.task_node import task
from takler.schema import DefinitionError
from takler.serialization.registry import NodeRegistration, builtin_registry

import pytest


def test_requeue_creates_a_new_attempt_with_the_same_try_number():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    node = flow.add_task("t")
    flow.begin()

    assert node.attempt_id is None
    assert node.definition_generation == flow.definition_generation
    node.run()
    first_attempt = node.attempt_id
    first_password = node.job_password
    assert str(UUID(first_attempt)) == first_attempt
    assert node.try_no == 1
    assert node.find_generated_parameter(TAKLER_ATTEMPT_ID).value == first_attempt
    assert node.find_generated_parameter(TAKLER_PASS).value == first_password
    assert node.find_generated_parameter(TAKLER_RID).value is None
    assert datetime.fromisoformat(node.submitted_at).tzinfo is not None

    node.init("external-rid")
    node.complete()
    assert node.task_id == "external-rid"
    assert datetime.fromisoformat(node.started_at).tzinfo is not None
    assert datetime.fromisoformat(node.ended_at).tzinfo is not None
    assert node.attempt_id == first_attempt

    node.requeue()
    assert node.attempt_id is None
    assert node.job_password is None
    assert node.submitted_at is node.started_at is node.ended_at is None
    assert node.find_generated_parameter(TAKLER_ATTEMPT_ID).value is None
    node.run()
    assert node.try_no == 1
    assert node.attempt_id != first_attempt
    assert node.job_password != first_password
    assert node.definition_generation == flow.definition_generation


def test_inline_task_uses_the_common_attempt_lifecycle():
    bunch = Bunch()
    flow = bunch.add_flow("f")
    inline = task("inline")(lambda self: None)()
    flow.add_task(inline)
    flow.begin()

    assert inline.run() is True
    assert inline.attempt_id is not None
    assert inline.submitted_at and inline.started_at and inline.ended_at
    assert inline.definition_generation == flow.definition_generation
    assert isinstance(inline, Task)


def test_registered_task_cannot_replace_the_common_run_entry():
    class BypassingTask(Task):
        def run(self):
            return True

    with pytest.raises(DefinitionError, match="invalid_registration"):
        builtin_registry().register(
            NodeRegistration("example.bypass", BypassingTask, "task")
        )


def test_task_run_allocates_identity_even_if_hook_is_overridden():
    class HookTask(Task):
        def before_run(self):
            assert self.attempt_id is not None

    node = HookTask("t")
    assert node.run() is True
    assert node.attempt_id is not None
