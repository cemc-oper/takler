"""Read one bounded initial query view, then one live detail group."""

from takler.client import TaklerServiceClient


def main() -> None:
    client = TaklerServiceClient()
    snapshot = client.query_snapshot(flow_name="forecast", depth=2)
    print(f"summary as_of={snapshot.as_of}, nodes={len(snapshot.ordered_paths)}")

    path = "/forecast/prepare"
    if snapshot.get(path) is not None:
        detail = client.query_detail(snapshot, path, ["parameters"])
        print(f"parameters as_of={detail.as_of}")
        for item in detail.groups.parameters.user:
            print(f"{item.name}={'<redacted>' if item.redacted else item.value}")


if __name__ == "__main__":
    main()
