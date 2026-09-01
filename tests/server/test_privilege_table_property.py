"""Property test for the completeness of the method-name privilege table.

``PRIVILEGE_BY_METHOD`` is hardcoded by fully qualified method name, so a new
rpc added to ``takler.proto`` without a matching entry would silently fall to
the runtime fallback in :func:`~takler.server.auth.privilege_for_method`. This
file turns that omission from a production surprise into a CI failure.

**No hypothesis here, and none is needed.** The input space of this property is
not "any string" but "the methods the ``TaklerServer`` service declares", and
that set is finite and fully enumerable from the protobuf service descriptor.
Walking the descriptor is therefore exhaustive by construction: a generator
could only sample the same set less completely. The two-line annotation format
is kept so the property stays traceable to its requirement.

The check runs in both directions, because the two failure modes are
different:

* every declared method is registered -- a missing entry means a new rpc was
  added without anyone classifying it;
* the table holds no key the service does not declare -- a stale key means a
  method was renamed and the old entry is now dead code that classifies
  nothing, while the new name resolves through the fallback.

The runtime fallback is asserted too. It is the safety net the design chose
deliberately (fail-closed: an unregistered method demands Operator
credentials), so a later refactor flipping it to ``PUBLIC`` would be a security
regression that no other test would catch.

Validates: Requirements 6.2
"""

from __future__ import annotations

import pytest

from takler.server.auth import (
    PRIVILEGE_BY_METHOD,
    SERVICE_METHOD_PREFIX,
    PrivilegeLevel,
    privilege_for_method,
)
from takler.server.protocol import takler_pb2


SERVICE_DESCRIPTOR = takler_pb2.DESCRIPTOR.services_by_name["TaklerServer"]

#: Fully qualified name of every method the service declares, in the form
#: ``handler_call_details.method`` carries at runtime and thus the form
#: ``PRIVILEGE_BY_METHOD`` is keyed by: a leading slash, the service full name,
#: a slash, the method name.
DECLARED_METHODS = tuple(
    f"/{SERVICE_DESCRIPTOR.full_name}/{method.name}"
    for method in SERVICE_DESCRIPTOR.methods
)


def test_descriptor_declares_methods():
    """Guard the guard: an empty descriptor walk would pass every assertion."""
    assert DECLARED_METHODS


def test_service_method_prefix_matches_the_descriptor():
    """The hardcoded key prefix is the one the descriptor spells out.

    Without this, a rename of the proto ``package`` or of the service would
    leave the table keyed by names no RPC ever carries, and the completeness
    check below would fail with a confusing "nothing is registered" instead of
    naming the cause.
    """
    assert SERVICE_METHOD_PREFIX == f"/{SERVICE_DESCRIPTOR.full_name}/"


# Feature: m2-security, Property 8: 方法名分级表的完备性
# Validates: Requirements 6.2
@pytest.mark.parametrize("method", DECLARED_METHODS)
def test_every_declared_method_is_registered(method):
    """Every rpc of ``TaklerServer`` has an explicit Privilege_Level entry."""
    assert method in PRIVILEGE_BY_METHOD, (
        f"{method} is declared in takler.proto but has no entry in "
        f"PRIVILEGE_BY_METHOD; classify it explicitly rather than relying on "
        f"the OPERATOR fallback"
    )
    assert isinstance(PRIVILEGE_BY_METHOD[method], PrivilegeLevel)


# Feature: m2-security, Property 8: 方法名分级表的完备性
# Validates: Requirements 6.2
def test_table_holds_no_method_the_service_does_not_declare():
    """No stale key survives a renamed or removed rpc."""
    stale = sorted(set(PRIVILEGE_BY_METHOD) - set(DECLARED_METHODS))

    assert stale == [], (
        f"PRIVILEGE_BY_METHOD registers method name(s) that takler.proto does "
        f"not declare: {stale}; a renamed method leaves the old key dead and "
        f"the new name unclassified"
    )


# Feature: m2-security, Property 8: 方法名分级表的完备性
# Validates: Requirements 6.2
def test_unregistered_method_falls_back_to_operator():
    """The runtime fallback is fail-closed, not fail-open."""
    unregistered = SERVICE_METHOD_PREFIX + "RunCommandNeverDeclared"
    assert unregistered not in PRIVILEGE_BY_METHOD

    level = privilege_for_method(unregistered)

    assert level is PrivilegeLevel.OPERATOR
    assert level is not PrivilegeLevel.PUBLIC
