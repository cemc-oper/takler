"""Independent process recording every external submission attempt."""

import os
import sys


def main() -> None:
    path, task_name = sys.argv[1:3]
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, f"{task_name}\n".encode())
        os.fsync(fd)
    finally:
        os.close(fd)


if __name__ == "__main__":
    main()
