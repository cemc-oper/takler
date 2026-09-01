"""Unit tests for the ``security`` section of :class:`ConnectConfig`.

Tasks 1.1 -- 1.3 of the *m2-security* spec add the Security_Settings model, the
:class:`AuthMode` / :class:`ZombiePolicy` enums and the ``resolve_*`` function
family. Four behaviors matter to callers:

* **Backward compatibility (Requirements 3.2, 16.10).** An M1 ``connect.yaml``
  -- holding only the ``server`` section, or only ``server`` and ``checkpoint``
  -- still loads and yields an all-``None`` :class:`SecuritySettings`. The two
  shapes are written here as literal YAML text rather than round-tripped
  through :func:`save_connect_config`, so a model change that breaks real files
  already sitting on disk fails this test instead of quietly agreeing with
  itself.
* **Source precedence (Requirements 3.4, 3.5).** Each ``resolve_*`` applies
  ``explicit argument > environment variable > Connect_Config ``security``
  section > built-in default``. Every level is asserted twice: once winning,
  once falling through when its value is blank. The blank rule is what makes an
  exported-but-empty variable harmless rather than shadowing the lower levels.
* **Graceful degradation (Requirements 3.6, 3.8, 3.9).** An unrecognized
  ``auth_mode`` / ``zombie_policy`` -- from any source -- degrades to the
  built-in default and emits a WARNING naming the offending value, rather than
  raising.
* **Generation (Requirement 3.10).** A freshly generated Connect_Config carries
  the ``security`` section, so a newly written file documents every knob.

Following ``tests/server/test_exception_policy_config.py``, the WARNING
assertions spy on the module-level ``server.config`` logger: that keeps them
independent of which logging backend is active and of pytest's stream capture.

Validates: Requirements 3.2, 3.4, 3.5, 3.6, 3.8, 3.9, 3.10, 16.10
"""

from __future__ import annotations

from unittest import mock

import pytest
import yaml

import takler.server.connect_config as connect_config
from takler.server.connect_config import (
    DEFAULT_AUTH_MODE,
    DEFAULT_ZOMBIE_POLICY,
    TAKLER_AUDIT_FILE,
    TAKLER_AUTH_MODE,
    TAKLER_ZOMBIE_POLICY,
    Address,
    AuthMode,
    ConnectConfig,
    SecuritySettings,
    Server,
    ZombiePolicy,
    generate_connect_config,
    load_connect_config,
    resolve_audit_file,
    resolve_auth_mode,
    resolve_zombie_policy,
    save_connect_config,
)


# The ten knobs of the ``security`` section (Requirement 3.3), listed
# explicitly so that adding or renaming a field is a deliberate change to this
# test rather than something a snapshot would absorb silently.
SECURITY_FIELDS = (
    "server_cert_file",
    "server_key_file",
    "client_ca_file",
    "ca_file",
    "server_name",
    "auth_mode",
    "operator_secret_file",
    "operator_whitelist_file",
    "zombie_policy",
    "audit_file",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _capture_warning(call):
    """Run ``call`` while spying on the module logger's ``warning`` method.

    Returns ``(result, warning_messages)`` where ``warning_messages`` is the
    list of fully-formatted WARNING strings emitted during the call.
    """
    with mock.patch.object(connect_config.logger, "warning") as warn:
        result = call()
    messages = [c.args[0] if c.args else "" for c in warn.call_args_list]
    return result, messages


# An M1 ``connect.yaml`` carrying only the ``server`` section, as written by
# takler v0.1.0 -- literal text, not a re-serialized model.
LEGACY_SERVER_ONLY_YAML = """\
server:
  address:
    hostname: login01
    ip: 10.0.0.11
    port: '33083'
"""

# An M1 ``connect.yaml`` carrying both sections that existed before M2.
LEGACY_SERVER_AND_CHECKPOINT_YAML = """\
checkpoint:
  file: /var/takler/takler.check
  interval: 45.5
server:
  address:
    hostname: login01
    ip: 10.0.0.11
    port: '33083'
"""


def _write(tmp_path, text: str):
    file_path = tmp_path / "connect.yaml"
    file_path.write_text(text)
    return file_path


def _assert_security_all_unset(settings: SecuritySettings) -> None:
    """Assert every Security_Settings knob reads as "not configured"."""
    assert set(type(settings).model_fields) == set(SECURITY_FIELDS)
    for field in SECURITY_FIELDS:
        assert getattr(settings, field) is None, field


def _config(**security) -> ConnectConfig:
    """A Connect_Config whose ``security`` section holds ``security``."""
    return ConnectConfig(
        server=Server(
            address=Address(hostname="login01", ip="10.0.0.11", port="33083")
        ),
        security=SecuritySettings(**security),
    )


# ---------------------------------------------------------------------------
# Backward compatible loading (Requirements 3.2, 16.10)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [LEGACY_SERVER_ONLY_YAML, LEGACY_SERVER_AND_CHECKPOINT_YAML],
    ids=["server-only", "server-and-checkpoint"],
)
def test_legacy_connect_yaml_loads_with_all_none_security(tmp_path, text: str) -> None:
    """An M1 ``connect.yaml`` loads unchanged (Requirements 3.2, 16.10)."""
    config = load_connect_config(_write(tmp_path, text))

    # The pre-M2 sections keep their meaning...
    assert config.server.address.hostname == "login01"
    assert config.server.address.ip == "10.0.0.11"
    assert config.server.address.port == "33083"
    # ...and the missing ``security`` section means "nothing configured"
    # rather than a load failure.
    _assert_security_all_unset(config.security)


def test_legacy_checkpoint_section_still_read(tmp_path) -> None:
    """Adding ``security`` does not disturb the ``checkpoint`` section."""
    config = load_connect_config(_write(tmp_path, LEGACY_SERVER_AND_CHECKPOINT_YAML))

    assert config.checkpoint.interval == 45.5
    assert config.checkpoint.file == "/var/takler/takler.check"


def test_security_section_values_are_read_back(tmp_path) -> None:
    """A file that does configure the section round-trips its values."""
    text = """\
server:
  address:
    hostname: login01
    ip: 10.0.0.11
    port: '33083'
security:
  auth_mode: enabled
  zombie_policy: adopt
  audit_file: /var/takler/audit.jsonl
  server_cert_file: /etc/takler/server.crt
  server_key_file: /etc/takler/server.key
"""

    config = load_connect_config(_write(tmp_path, text))

    assert config.security.auth_mode == "enabled"
    assert config.security.zombie_policy == "adopt"
    assert config.security.audit_file == "/var/takler/audit.jsonl"
    assert config.security.server_cert_file == "/etc/takler/server.crt"
    assert config.security.server_key_file == "/etc/takler/server.key"
    # Knobs the file leaves out stay "not configured".
    assert config.security.client_ca_file is None
    assert config.security.operator_secret_file is None


def test_default_security_section_is_not_shared() -> None:
    """Each :class:`ConnectConfig` gets its own ``security`` instance."""
    first = _config()
    second = _config()

    first.security.auth_mode = "enabled"

    assert second.security.auth_mode is None


# ---------------------------------------------------------------------------
# Built-in defaults and env var names (Requirements 3.6, 3.7)
# ---------------------------------------------------------------------------


def test_built_in_defaults() -> None:
    """Auth_Mode defaults to ``disabled``, Zombie_Policy to ``fail``."""
    assert DEFAULT_AUTH_MODE is AuthMode.DISABLED
    assert DEFAULT_ZOMBIE_POLICY is ZombiePolicy.FAIL


def test_env_var_constant_names() -> None:
    """The three env var constants match their documented names."""
    assert TAKLER_AUTH_MODE == "TAKLER_AUTH_MODE"
    assert TAKLER_ZOMBIE_POLICY == "TAKLER_ZOMBIE_POLICY"
    assert TAKLER_AUDIT_FILE == "TAKLER_AUDIT_FILE"


# ---------------------------------------------------------------------------
# resolve_auth_mode -- four level precedence (Requirements 3.4, 3.5)
# ---------------------------------------------------------------------------


def test_resolve_auth_mode_explicit_wins() -> None:
    """Level 1: an explicit argument beats env var and config file."""
    env = {TAKLER_AUTH_MODE: "disabled"}
    config = _config(auth_mode="disabled")

    assert (
        resolve_auth_mode(explicit="enabled", connect_config=config, env=env)
        is AuthMode.ENABLED
    )
    # An already parsed member is honored too.
    assert (
        resolve_auth_mode(explicit=AuthMode.ENABLED, connect_config=config, env=env)
        is AuthMode.ENABLED
    )


def test_resolve_auth_mode_env_wins_over_config() -> None:
    """Level 2: with no explicit argument, the env var beats the config file."""
    env = {TAKLER_AUTH_MODE: "enabled"}
    config = _config(auth_mode="disabled")

    assert resolve_auth_mode(connect_config=config, env=env) is AuthMode.ENABLED


def test_resolve_auth_mode_config_wins_over_default() -> None:
    """Level 3: with no explicit argument and no env var, the file applies."""
    config = _config(auth_mode="enabled")

    assert resolve_auth_mode(connect_config=config, env={}) is AuthMode.ENABLED


def test_resolve_auth_mode_default_when_nothing_set() -> None:
    """Level 4: nothing configured anywhere yields the built-in default."""
    assert resolve_auth_mode(connect_config=None, env={}) is AuthMode.DISABLED
    assert resolve_auth_mode(connect_config=_config(), env={}) is AuthMode.DISABLED


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_resolve_auth_mode_blank_falls_through_each_level(blank: str) -> None:
    """A blank value at any level defers to the next one, never shadows it."""
    # Blank explicit -> env var.
    assert (
        resolve_auth_mode(
            explicit=blank,
            connect_config=_config(auth_mode="disabled"),
            env={TAKLER_AUTH_MODE: "enabled"},
        )
        is AuthMode.ENABLED
    )
    # Blank env var -> config file.
    assert (
        resolve_auth_mode(
            connect_config=_config(auth_mode="enabled"),
            env={TAKLER_AUTH_MODE: blank},
        )
        is AuthMode.ENABLED
    )
    # Blank config file value -> built-in default.
    assert (
        resolve_auth_mode(connect_config=_config(auth_mode=blank), env={})
        is AuthMode.DISABLED
    )


def test_resolve_auth_mode_tolerates_case_and_whitespace() -> None:
    """Names parse case-insensitively with surrounding whitespace ignored."""
    assert (
        resolve_auth_mode(connect_config=_config(auth_mode=" Enabled "), env={})
        is AuthMode.ENABLED
    )
    assert resolve_auth_mode(env={TAKLER_AUTH_MODE: "ENABLED"}) is AuthMode.ENABLED


# ---------------------------------------------------------------------------
# resolve_zombie_policy -- four level precedence (Requirements 3.4, 3.5)
# ---------------------------------------------------------------------------


def test_resolve_zombie_policy_explicit_wins() -> None:
    """Level 1: an explicit argument beats env var and config file."""
    env = {TAKLER_ZOMBIE_POLICY: "fob"}
    config = _config(zombie_policy="fail")

    assert (
        resolve_zombie_policy(explicit="adopt", connect_config=config, env=env)
        is ZombiePolicy.ADOPT
    )
    assert (
        resolve_zombie_policy(
            explicit=ZombiePolicy.ADOPT, connect_config=config, env=env
        )
        is ZombiePolicy.ADOPT
    )


def test_resolve_zombie_policy_env_wins_over_config() -> None:
    """Level 2: with no explicit argument, the env var beats the config file."""
    env = {TAKLER_ZOMBIE_POLICY: "adopt"}
    config = _config(zombie_policy="fob")

    assert resolve_zombie_policy(connect_config=config, env=env) is ZombiePolicy.ADOPT


def test_resolve_zombie_policy_config_wins_over_default() -> None:
    """Level 3: with no explicit argument and no env var, the file applies."""
    config = _config(zombie_policy="fob")

    assert resolve_zombie_policy(connect_config=config, env={}) is ZombiePolicy.FOB


def test_resolve_zombie_policy_default_when_nothing_set() -> None:
    """Level 4: nothing configured anywhere yields the built-in default."""
    assert resolve_zombie_policy(connect_config=None, env={}) is ZombiePolicy.FAIL
    assert resolve_zombie_policy(connect_config=_config(), env={}) is ZombiePolicy.FAIL


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_resolve_zombie_policy_blank_falls_through_each_level(blank: str) -> None:
    """A blank value at any level defers to the next one, never shadows it."""
    # Blank explicit -> env var.
    assert (
        resolve_zombie_policy(
            explicit=blank,
            connect_config=_config(zombie_policy="fail"),
            env={TAKLER_ZOMBIE_POLICY: "adopt"},
        )
        is ZombiePolicy.ADOPT
    )
    # Blank env var -> config file.
    assert (
        resolve_zombie_policy(
            connect_config=_config(zombie_policy="fob"),
            env={TAKLER_ZOMBIE_POLICY: blank},
        )
        is ZombiePolicy.FOB
    )
    # Blank config file value -> built-in default.
    assert (
        resolve_zombie_policy(connect_config=_config(zombie_policy=blank), env={})
        is ZombiePolicy.FAIL
    )


# ---------------------------------------------------------------------------
# resolve_audit_file -- four level precedence (Requirements 3.4, 3.5)
# ---------------------------------------------------------------------------


def test_resolve_audit_file_explicit_wins() -> None:
    """Level 1: an explicit argument beats env var and config file."""
    env = {TAKLER_AUDIT_FILE: "/env/audit.jsonl"}
    config = _config(audit_file="/config/audit.jsonl")

    assert (
        resolve_audit_file(
            explicit="/explicit/audit.jsonl", connect_config=config, env=env
        )
        == "/explicit/audit.jsonl"
    )


def test_resolve_audit_file_env_wins_over_config() -> None:
    """Level 2: with no explicit argument, the env var beats the config file."""
    env = {TAKLER_AUDIT_FILE: "/env/audit.jsonl"}
    config = _config(audit_file="/config/audit.jsonl")

    assert resolve_audit_file(connect_config=config, env=env) == "/env/audit.jsonl"


def test_resolve_audit_file_config_wins_over_default() -> None:
    """Level 3: with no explicit argument and no env var, the file applies."""
    config = _config(audit_file="/config/audit.jsonl")

    assert resolve_audit_file(connect_config=config, env={}) == "/config/audit.jsonl"


def test_resolve_audit_file_default_is_no_audit_file() -> None:
    """Level 4: nothing configured anywhere means "no Audit_File"."""
    assert resolve_audit_file(connect_config=None, env={}) is None
    assert resolve_audit_file(connect_config=_config(), env={}) is None


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_resolve_audit_file_blank_falls_through_each_level(blank: str) -> None:
    """A blank value at any level defers to the next one, never shadows it."""
    # Blank explicit -> env var.
    assert (
        resolve_audit_file(
            explicit=blank,
            connect_config=_config(audit_file="/config/audit.jsonl"),
            env={TAKLER_AUDIT_FILE: "/env/audit.jsonl"},
        )
        == "/env/audit.jsonl"
    )
    # Blank env var -> config file.
    assert (
        resolve_audit_file(
            connect_config=_config(audit_file="/config/audit.jsonl"),
            env={TAKLER_AUDIT_FILE: blank},
        )
        == "/config/audit.jsonl"
    )
    # Blank config file value -> no Audit_File.
    assert resolve_audit_file(connect_config=_config(audit_file=blank), env={}) is None


def test_resolve_audit_file_strips_surrounding_whitespace() -> None:
    """A path is normalized by trimming surrounding whitespace only."""
    assert resolve_audit_file(explicit="  /a/audit.jsonl\t") == "/a/audit.jsonl"
    assert (
        resolve_audit_file(env={TAKLER_AUDIT_FILE: " /b/audit.jsonl "})
        == "/b/audit.jsonl"
    )
    assert (
        resolve_audit_file(
            connect_config=_config(audit_file=" /c/audit.jsonl "), env={}
        )
        == "/c/audit.jsonl"
    )


# ---------------------------------------------------------------------------
# Invalid values degrade with a WARNING (Requirements 3.8, 3.9)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["enable", "on", "true", "ENABLE_ALL"])
def test_invalid_auth_mode_degrades_to_disabled(bad: str) -> None:
    """An unrecognized Auth_Mode degrades to ``disabled`` (Requirement 3.8)."""
    result, warnings = _capture_warning(lambda: AuthMode.from_str(bad))

    assert result is AuthMode.DISABLED
    assert len(warnings) == 1
    assert repr(bad) in warnings[0]


def test_invalid_auth_mode_from_env_degrades_with_warning() -> None:
    """An unrecognized env value degrades and names the offending value."""
    result, warnings = _capture_warning(
        lambda: resolve_auth_mode(env={TAKLER_AUTH_MODE: "bogus-mode"})
    )

    assert result is AuthMode.DISABLED
    assert len(warnings) == 1
    assert repr("bogus-mode") in warnings[0]


def test_invalid_auth_mode_from_config_degrades_with_warning() -> None:
    """An unrecognized value in the ``security`` section degrades the same way."""
    result, warnings = _capture_warning(
        lambda: resolve_auth_mode(
            connect_config=_config(auth_mode="bogus-mode"), env={}
        )
    )

    assert result is AuthMode.DISABLED
    assert len(warnings) == 1
    assert repr("bogus-mode") in warnings[0]


def test_invalid_auth_mode_explicit_degrades_with_warning() -> None:
    """An unrecognized explicit value degrades rather than raising."""
    result, warnings = _capture_warning(
        lambda: resolve_auth_mode(explicit="bogus-mode", env={})
    )

    assert result is AuthMode.DISABLED
    assert len(warnings) == 1


@pytest.mark.parametrize("bad", ["failed", "adopt-all", "ignore", "0"])
def test_invalid_zombie_policy_degrades_to_fail(bad: str) -> None:
    """An unrecognized Zombie_Policy degrades to ``fail`` (Requirement 3.9)."""
    result, warnings = _capture_warning(lambda: ZombiePolicy.from_str(bad))

    assert result is ZombiePolicy.FAIL
    assert len(warnings) == 1
    assert repr(bad) in warnings[0]


def test_invalid_zombie_policy_from_env_degrades_with_warning() -> None:
    """An unrecognized env value degrades and names the offending value."""
    result, warnings = _capture_warning(
        lambda: resolve_zombie_policy(env={TAKLER_ZOMBIE_POLICY: "bogus-policy"})
    )

    assert result is ZombiePolicy.FAIL
    assert len(warnings) == 1
    assert repr("bogus-policy") in warnings[0]


def test_invalid_zombie_policy_from_config_degrades_with_warning() -> None:
    """An unrecognized value in the ``security`` section degrades the same way."""
    result, warnings = _capture_warning(
        lambda: resolve_zombie_policy(
            connect_config=_config(zombie_policy="bogus-policy"), env={}
        )
    )

    assert result is ZombiePolicy.FAIL
    assert len(warnings) == 1
    assert repr("bogus-policy") in warnings[0]


def test_valid_values_emit_no_warning() -> None:
    """A recognized value at any level is resolved without a WARNING."""
    _, warnings = _capture_warning(
        lambda: (
            resolve_auth_mode(connect_config=_config(auth_mode="enabled"), env={}),
            resolve_zombie_policy(env={TAKLER_ZOMBIE_POLICY: "adopt"}),
            resolve_audit_file(explicit="/a/audit.jsonl"),
        )
    )

    assert warnings == []


# ---------------------------------------------------------------------------
# Generation (Requirement 3.10)
# ---------------------------------------------------------------------------


def test_generate_connect_config_includes_security_section(monkeypatch) -> None:
    """A generated Connect_Config carries the ``security`` section.

    :func:`generate_connect_config` normally reaches the network to discover
    this host's IP and a free port; both lookups are stubbed out here so the
    test stays hermetic and asserts on the model only.
    """
    monkeypatch.setattr(connect_config.socket, "gethostname", lambda: "login01")
    monkeypatch.setattr(connect_config, "get_ip", lambda: "10.0.0.11")
    monkeypatch.setattr(connect_config, "get_port", lambda: 33083)

    config = generate_connect_config()

    assert config.server.address.hostname == "login01"
    assert config.server.address.ip == "10.0.0.11"
    assert config.server.address.port == "33083"
    # The section is present with every knob left at "not configured", so a
    # freshly generated file documents all of them.
    _assert_security_all_unset(config.security)


def test_generated_config_writes_security_section_to_file(
    tmp_path, monkeypatch
) -> None:
    """The written file holds a ``security`` mapping with all ten knobs."""
    monkeypatch.setattr(connect_config.socket, "gethostname", lambda: "login01")
    monkeypatch.setattr(connect_config, "get_ip", lambda: "10.0.0.11")
    monkeypatch.setattr(connect_config, "get_port", lambda: 33083)
    file_path = tmp_path / "connect.yaml"

    save_connect_config(generate_connect_config(), file_path)
    written = yaml.safe_load(file_path.read_text())

    assert "security" in written
    assert written["security"] == dict.fromkeys(SECURITY_FIELDS, None)
    # ...and the file it produced is loadable again.
    _assert_security_all_unset(load_connect_config(file_path).security)
