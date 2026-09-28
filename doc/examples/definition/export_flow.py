"""Export a definition without contacting a server or starting a job."""

import argparse
from pathlib import Path

from takler.core import Flow, NodeStatus
from takler.serialization import export_definition


def create_flow():
    flow = Flow("forecast")
    # This demonstration task stays complete after begin; it starts no job.
    flow.add_task("prepare").set_default_node_status(NodeStatus.complete)
    return flow


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    document = export_definition(create_flow())
    args.output.write_text(document.model_dump_json(indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
