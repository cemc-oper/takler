"""Entry point: ``python -m takler.server``.

Equivalent to the ``takler-server`` console script; both go through
:func:`takler.server.cli.main`.
"""

from takler.server.cli import main


if __name__ == "__main__":
    main()
