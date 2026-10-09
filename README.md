# takler

![Maturity-Sandbox](https://img.shields.io/badge/Maturity-Sandbox-F9D71C)
![GitHub Release](https://img.shields.io/github/v/release/cemc-oper/takler)
![PyPI - Version](https://img.shields.io/pypi/v/takler)
[![Documentation Status](https://readthedocs.org/projects/takler/badge/?version=latest)](https://takler.readthedocs.io/zh_CN/latest/?badge=latest)
![ci](https://github.com/cemc-oper/takler/actions/workflows/ci.yml/badge.svg)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/9900bc93e2a540d0926aca37a62c4a62)](https://app.codacy.com/gh/cemc-oper/takler/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

> :construction: takler is under construction.

A task scheduler tool for Numerical Weather Prediction (NWP) workflows.

Chinese documentation (中文文档)：[https://takler.readthedocs.io/](https://takler.readthedocs.io/)

## Quick Start

Install takler with pip (Python 3.11 or newer):

```bash
pip install takler
```

For the HTTP listener, install `takler[http]`; for the terminal interface,
install `takler[tui]`. The Go client is built separately from the
`takler-client` repository.

### 1. Define a flow

Create **test.py**, which defines a flow named `test` with two shell tasks.
The trigger on `t2` makes it wait until `t1` completes:

```python
import asyncio
import sys
from pathlib import Path

from takler.core import Flow
from takler.server import TaklerServer
from takler.tasks.shell import ShellScriptTask
from takler.visitor import pre_order_travel, PrintVisitor

TAKLER_HOME = Path(__file__).parent


def create_flow():
    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(TAKLER_HOME))

    task1 = flow.add_task(ShellScriptTask("t1"))
    task1.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/t1.takler")))

    task2 = flow.add_task(ShellScriptTask("t2"))
    task2.add_parameter("TAKLER_SCRIPT", str(Path(TAKLER_HOME, "test/t2.takler")))
    task2.add_trigger("./t1 == complete")

    return flow


async def run_takler(server):
    await server.start()
    await server.run()


def main():
    flow = create_flow()
    pre_order_travel(flow, PrintVisitor(sys.stdout))

    server = TaklerServer()
    server.bunch.add_flow(flow)
    asyncio.run(run_takler(server))


if __name__ == "__main__":
    main()
```

### 2. Write the task scripts

Each `ShellScriptTask` renders a `.takler` script (a Jinja2 template) into a job script and runs it. 
The script calls back to the server with *child commands*: `init` when it starts and `complete` when it finishes. 
Create **test/t1.takler**:

```bash
#!/bin/bash
set -e

export TAKLER_HOST={{ TAKLER_HOST }}
export TAKLER_PORT={{ TAKLER_PORT }}
export TAKLER_NAME={{ TAKLER_NAME }}
export TAKLER_ATTEMPT_ID={{ TAKLER_ATTEMPT_ID }}
export TAKLER_PASS={{ TAKLER_PASS }}

takler-client-py init --task-id "$$"

echo "hello from t1"
sleep 2

takler-client-py complete
```

and **test/t2.takler** the same way, with `echo "hello from t2"` as the payload.
`TAKLER_HOST`, `TAKLER_PORT` and `TAKLER_NAME` are parameters takler generates for every task;
the client used inside the script can be either `takler-client-py` (installed with takler) or the Go client `takler_client`,
as long as it is on the job's `PATH`. Both clients read `TAKLER_ATTEMPT_ID`
to identify this execution attempt and `TAKLER_PASS` for child authentication;
`--task-id` reports the job ID (`TAKLER_RID` in shared script templates).

### 3. Run it

Start the server (it listens on `localhost:33083` by default):

```bash
python test.py
```

In another terminal, begin the flow and requeue it so the scheduler picks it up:

```bash
takler-client-py begin test      # begin acts on a flow name, not a path
takler-client-py requeue /test
takler-client-py show
```

`t1` runs first; once it reports `complete`, the trigger releases `t2`, and finally the whole flow reaches `complete`:

```
|- test [complete]
  |- t1 [complete]
  |- t2 [complete]
```

For events, meters, time dependencies, families and more, see the [documentation](https://takler.readthedocs.io/) (中文文档).

### Query a large tree

`show` reads a bounded, paged summary with QueryDocument v1. Select a scope
before requesting per-node details:

```bash
takler-client-py show --scope /test
takler-client-py show --scope /test/t1 --show-parameter --show-trigger
takler-client-py sync --scope /test --polls 5 --interval 1
takler-tui --scope /test
```

The summary has one `as_of` time. Requested details are sampled later and
marked as live; they are not a second copy of that snapshot. The TUI loads
details when a node is selected and retains the last complete view if a refresh
fails. Later refreshes merge revision deltas into visible rows, and a reset
rebuilds the selected scope. Query caches do not survive server restarts.
The old full-tree `RunRequestShow` RPC and HTTP `show` endpoint have been
removed; upgrade the server and clients together. See the [query guide](doc/source/guide/query.rst)
for pagination, limits, errors and the distinction from checkpoints.

## Recovery and control

`server-status` shows whether the service is running or halted;
`server-halt` stops new automatic and manual execution, including `run --force`.
After a checkpoint restore, the service starts halted. Inspect the current
attempts and external jobs with `server-status` and `show`, correct confirmed
states with `force` or `requeue`, then use `server-resume`. A node's `resume`
does not resume the service.

Checkpoint v3 stores definition, runtime state, and in-flight job passwords in
separate sections. It is the only recovery source; logs are for diagnosis and
audit. `Bunch.server_config` holds the current deployment address, while
`Bunch.server_state` holds the checkpointed running/halted state and causes.
A successful write command changes in-memory state and may be lost
before the next checkpoint. Mutating commands, including child reports, are
sent once; after a lost response, query the state before deciding whether to
send another command. `async_task` is explicitly unsupported.

## Release verification

The **release** workflow checks the tag's actual commit against the latest
successful main-branch CI run and every required job before building. Its
sdist and wheel are installed and checked, then listed with SHA-256 hashes and
the source commit in `release-manifest.json`. Run the workflow manually with an
existing remote tag and the default `dry_run` setting to validate without
uploading to PyPI. Publication rechecks the downloaded artifact and tag commit.

## LICENSE

Copyright &copy; 2022-2026, developers at cemc-oper.

*takler* is licensed under [Apache License, Version 2.0](./LICENSE)

<span style="color:#01665e">t</span><span style="color:#5ab4ac">a</span><span style="color:#c7eae5">k</span><span style="color:#f6e8c3">l</span><span style="color:#d8b365">e</span><span style="color:#8c510a">r</span>
