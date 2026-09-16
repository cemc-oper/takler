"""Unit tests for the host/port the Takler_Server announces to its Bunch.

The bunch's Server_State falls back to ``DEFAULT_HOST`` / ``DEFAULT_PORT``
when constructed with ``None``, but Takler_Server used to stringify its
``port`` argument unconditionally, so ``TaklerServer()`` announced the literal
string ``"None"`` as ``TAKLER_PORT`` and every generated job script tried to
reach ``localhost:None``. These tests pin the pass-through behaviour for the
three constructor shapes: no arguments, an int port, and a string port.
"""

from takler.constant import DEFAULT_HOST, DEFAULT_PORT
from takler.core.parameter import TAKLER_HOST, TAKLER_PORT
from takler.server import TaklerServer


def _announced(server: TaklerServer) -> dict:
    return {
        p.name: p.value for p in server.bunch.server_state.server_parameters
    }


def test_default_arguments_announce_default_host_and_port():
    server = TaklerServer()

    assert server.bunch.server_state.host == DEFAULT_HOST
    assert server.bunch.server_state.port == DEFAULT_PORT
    announced = _announced(server)
    assert announced[TAKLER_HOST] == DEFAULT_HOST
    assert announced[TAKLER_PORT] == DEFAULT_PORT


def test_int_port_is_announced_as_string():
    server = TaklerServer(host="login01", port=44084)

    assert server.bunch.server_state.port == "44084"
    assert _announced(server)[TAKLER_PORT] == "44084"


def test_string_port_is_announced_unchanged():
    server = TaklerServer(host="login01", port="44084")

    assert server.bunch.server_state.port == "44084"
    assert _announced(server)[TAKLER_PORT] == "44084"
