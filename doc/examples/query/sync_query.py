"""Poll a bounded summary with QueryDocument v1 revision deltas."""

from takler.client import TaklerServiceClient


def main() -> None:
    client = TaklerServiceClient()
    snapshot = client.query_sync(flow_name="forecast", depth=2, polls=5, interval=1.0)
    print(f"revision={snapshot.revision}, nodes={len(snapshot.ordered_paths)}")
    for path in snapshot.ordered_paths:
        node = snapshot.get(path)
        if node is not None:
            print(f"{path}: {node.status}")


if __name__ == "__main__":
    main()
