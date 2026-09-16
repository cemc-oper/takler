# takler

![Maturity-Sandbox](https://img.shields.io/badge/Maturity-Sandbox-F9D71C)
![GitHub Release](https://img.shields.io/github/v/release/cemc-oper/takler)
![PyPI - Version](https://img.shields.io/pypi/v/takler)
[![Documentation Status](https://readthedocs.org/projects/takler/badge/?version=latest)](https://takler.readthedocs.io/zh_CN/latest/?badge=latest)
![test](https://github.com/cemc-oper/takler/actions/workflows/test.yml/badge.svg)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/9900bc93e2a540d0926aca37a62c4a62)](https://app.codacy.com/gh/cemc-oper/takler/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

> :construction: takler is under construction.

A task scheduler tool for Numerical Weather Prediction (NWP) workflows.

Chinese documentation (中文文档)：[https://takler.readthedocs.io/](https://takler.readthedocs.io/)

## Quick Start

Install takler with pip:

```bash
pip install takler
```

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

Each `ShellScriptTask` renders a `.takler` script (a Jinja2 template) into a
job script and runs it. The script calls back to the server with *child
commands*: `init` when it starts and `complete` when it finishes. Create
**test/t1.takler**:

```bash
#!/bin/bash
set -e

takler-client-py init --host {{ TAKLER_HOST }} --port {{ TAKLER_PORT }} \
    --task-id $$ --node-path {{ TAKLER_NAME }}

echo "hello from t1"
sleep 2

takler-client-py complete --host {{ TAKLER_HOST }} --port {{ TAKLER_PORT }} \
    --node-path {{ TAKLER_NAME }}
```

and **test/t2.takler** the same way, with `echo "hello from t2"` as the
payload. `TAKLER_HOST`, `TAKLER_PORT` and `TAKLER_NAME` are parameters takler
generates for every task; the client used inside the script can be either
`takler-client-py` (installed with takler) or the Go client `takler_client`,
as long as it is on the job's `PATH`.

### 3. Run it

Start the server (it listens on `localhost:33083` by default):

```bash
python test.py
```

In another terminal, begin the flow and requeue it so the scheduler picks it
up:

```bash
takler-client-py begin test      # begin acts on a flow name, not a path
takler-client-py requeue /test
takler-client-py show
```

`t1` runs first; once it reports `complete`, the trigger releases `t2`, and
finally the whole flow reaches `complete`:

```
|- test [complete]
  |- t1 [complete]
  |- t2 [complete]
```

For events, meters, time dependencies, families and more, see the
[documentation](https://takler.readthedocs.io/) (中文文档).

## Documentation

### Build documentation locally

Documentation dependencies (Sphinx, pydata-sphinx-theme, etc.) live in the `docs`
dependency group in `pyproject.toml`, kept separate from the `dev` group so a docs
build doesn't also pull in the linter or test tooling. Build the HTML docs with:

```bash
uv run --group docs sphinx-build -b html doc/source doc/build/html
```

or, using the Sphinx Makefile:

```bash
cd doc
uv run --project .. --group docs make html
```

The generated HTML is written to `doc/build/html/index.html`. This is the same
`uv sync --group docs` toolchain that Read the Docs uses (see `.readthedocs.yml`).

## LICENSE

Copyright &copy; 2022-2025, developers at cemc-oper.

*takler* is licensed under [Apache License, Version 2.0](./LICENSE)

<span style="color:#01665e">t</span><span style="color:#5ab4ac">a</span><span style="color:#c7eae5">k</span><span style="color:#f6e8c3">l</span><span style="color:#d8b365">e</span><span style="color:#8c510a">r</span>