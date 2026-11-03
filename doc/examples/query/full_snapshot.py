"""Read public fields from one fixed version; streaming output needs completion."""

import argparse
from contextlib import closing
from dataclasses import asdict
import json
import sys

from takler.client import TaklerServiceClient
from takler.client.full_snapshot import FullSnapshotComplete


def write_snapshot(client, scope, stream, output):
    if not stream:
        view = client.read_full_snapshot(scope_path=scope)
        json.dump(asdict(view), output, ensure_ascii=False)
        output.write("\n")
        return

    complete = False
    # closing also releases the iterator if the output writer fails.
    with closing(client.iter_full_snapshot(scope_path=scope)) as items:
        for item in items:
            if isinstance(item, FullSnapshotComplete):
                record = {"complete": asdict(item)}
                complete = True
            else:
                record = {"node": item}
            output.write(json.dumps(record, ensure_ascii=False) + "\n")
    if not complete:
        raise RuntimeError("incomplete full snapshot")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=33083)
    parser.add_argument("--transport", choices=["grpc", "http"], default="grpc")
    parser.add_argument("--scope", default="/")
    parser.add_argument("--stream", action="store_true")
    args = parser.parse_args()
    client = TaklerServiceClient(
        host=args.host, port=args.port, transport_name=args.transport
    )
    write_snapshot(client, args.scope, args.stream, sys.stdout)


if __name__ == "__main__":
    main()
