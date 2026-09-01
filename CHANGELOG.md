# Changelog

This file records the notable changes in each release of takler.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the version numbers follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

This is the M2 "security" stage. With the default configuration (`Auth_Mode=disabled`, no TLS
configured), an existing M1 deployment can be upgraded without touching `connect.yaml`, and an old
Checkpoint_File needs no migration. But **two behaviour changes are directly visible to existing
deployments**, so please read the "Changed" section below before upgrading.

### Added

- **gRPC TLS transport encryption.** The server can be configured with a certificate and a private
  key, and both the Python client and the Go client can be configured with a CA certificate and a
  server name override. TLS is off by default so that existing deployments keep working; when it is
  not enabled the server logs a WARNING at startup. The client CA certificate option leaves room for
  mTLS later, but this release does not verify client certificates.
- **One-time job password `TAKLER_PASS`.** On `increment_try_no` the server generates a one-time
  password for each run of a task (from a cryptographically secure random source, at least 32
  characters long) and injects it into the job script as a Task level generated variable; `requeue`
  clears it. The password is persisted with the checkpoint (only for tasks in the submitted or
  active state), so restarting the server does not turn in-flight jobs into zombies. The password
  never appears in node serialization results, in `show` responses, in audit records, or in logs at
  any level.
- **Command classification and authentication.** A new server-side authentication switch `Auth_Mode`
  (default `disabled`). Once enabled: child commands (`init` / `complete` / `abort` / `event` /
  `meter`) must carry the job password; control commands and `show` / `coroutine` must carry the
  operator shared secret and the caller's user name must be in the allow list; `ping` needs no
  authentication and can be used for health checking and monitoring. Credentials travel in gRPC
  metadata (`takler-pass` / `takler-secret` / `takler-user`), and `takler.proto` is unchanged.
  Validation happens in a single gRPC interceptor, and a rejected RPC never reaches the handler. The
  shared secret file may contain several lines, which makes it possible to rotate the secret without
  stopping the server.
- **Zombie detection and handling.** Three detection conditions: `Z1` (the password does not match
  the current run instance, only evaluated when `Auth_Mode=enabled`), `Z2` (the target task is
  neither submitted nor active) and `Z3` (the `task_id` carried by `init` does not match the recorded
  one). The server-wide handling policy `Zombie_Policy` takes the values `fail` (default), `fob`
  (return success silently) or `adopt` (adopt the job).
- **Audit log.** Every control command, every authentication rejection and every zombie handling
  writes one JSON Lines record, containing `timestamp`, `event`, `command`, `user`, `peer`, `target`,
  `outcome` and `error_code`. `Audit_File` can direct the records to a dedicated file, which is
  created with owner-only read and write permissions.
- **Security configuration section.** `connect.yaml` gains a `security` section that gathers the four
  groups of options for TLS, authentication, zombies and auditing; an existing configuration file
  with only a `server` section and a `checkpoint` section still loads and takes the built-in
  defaults. New environment variables: `TAKLER_PASS`, `TAKLER_SECRET_FILE`, `TAKLER_TLS_CA_FILE`,
  `TAKLER_TLS_SERVER_NAME`, `TAKLER_AUTH_MODE`, `TAKLER_ZOMBIE_POLICY`, `TAKLER_AUDIT_FILE`.
- **Security deployment documentation.** A new one-page security deployment guide, covering
  certificate configuration, the format and permission requirements of the secret and allow list
  files, the order of the steps to upgrade an M1 deployment to enabled authentication, and the
  procedure for rotating the shared secret without downtime.

### Changed

- **takler no longer sets the job script file mode explicitly.** `ShellScriptTask.create_job_script`
  used to `chmod(0o755)` the rendered job script; now it only adds the owner execute bit to the mode
  the file received when it was created (`mode | stat.S_IXUSR`), and the read and write bits are left
  entirely to the process umask. Under the common umask `0022` the job script changes from `0755` to
  `0744`; under umask `0077` it becomes `0700`. The reason for the change is that the job script
  exports `TAKLER_PASS`, so who can read the job password has to be a decision the deployment
  expresses through its umask rather than something takler hard-codes.
  **Impact:** deployments that rely on the job script being executable by the group or by others (for
  example starting job scripts under a different account) need to widen the umask of the server
  process accordingly; conversely, when `Auth_Mode` is enabled the umask of the server process must
  be set to `0077` or an equivalent value, otherwise other accounts on a shared file system can read
  the password of an in-flight job and the authentication is worthless. When `Auth_Mode=enabled` and
  the umask is too wide, the server logs a WARNING at startup that names the current umask and the
  recommended value.
- **The `Z2` / `Z3` zombie checks also apply when `Auth_Mode=disabled`.** Only `Z1`, which depends on
  the password, is skipped when authentication is not enabled. So with the default configuration, a
  child command reported by the old job of a task that has been `requeue`d now hits `Z2` (the task is
  back to queued, neither submitted nor active), where M1 would have executed the command as usual
  and polluted the state of the new instance. **Impact:** under the default policy
  `Zombie_Policy=fail`, the server does not change the target task's state, `task_id`, `try_no`,
  `aborted_reason` or password, and returns `flag=31`; the client accordingly ends the process with
  exit code 3 and writes one line to standard error containing the error classification name `zombie`
  and the server message. In other words, the line where an old job calls a child command now fails
  with a non-zero exit code, and if the job script has `set -e` enabled it terminates there. That is
  exactly the effect M2 aims for, but it turns old jobs that used to "succeed silently" into visible
  failures. To keep the M1 behaviour of passing silently, set `Zombie_Policy` to `fob` (which returns
  `flag=0` and likewise does not change node state).
- **The Checkpoint_File and the audit file are created with owner-only read and write permissions
  (`0600`)**, because the snapshot contains the passwords of in-flight jobs. When a snapshot has to
  be handed to someone else for investigation, `jq 'del(.job_passwords)'` can strip them first.

### Go client

The Go client `takler_client` lives in the separate `takler-client` repository and does not ship with
this Python package. Besides filling in TLS and credential injection, this stage also filled in the
M1 client robustness it was missing entirely, so that it follows the same contract as the Python
client:

- a single call timeout (10 seconds by default) and exponential backoff retries
  (`min(2 ** (n - 1), 60)` seconds), with the retry window controlled by `TAKLER_TIMEOUT`, which when
  unset is 86400 seconds for child commands and 60 seconds for control and query commands;
- the exit code convention: 0 on success, 1 for a request error, 3 for a server error and for
  zombies, 4 when the server stays unreachable for the whole retry window; `log.Fatalf` no longer
  appears on the command execution path;
- it prints the classification name of the Error_Code instead of the bare `flag` integer, with a
  mapping table identical to the Python side and pinned down by tests on both ends;
- it supports `NO_TAKLER`, skipping the communication and exiting with code 0 just like the Python
  client;
- new Go test infrastructure (an in-process gRPC test server); the repository previously had no test
  files at all.

## [0.1.0]

The first release of the M1 "operable baseline" stage.
