from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("takler")
except PackageNotFoundError:
    # package is not installed
    pass
