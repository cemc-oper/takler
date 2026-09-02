from takler.client import TaklerServiceClient


def main():
    client = TaklerServiceClient()
    client.ping()

    client.set_host_port("remote-host", "33083")
    client.ping()


if __name__ == "__main__":
    main()
