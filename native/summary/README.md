# Optional Rust summary receiver

This distribution accelerates only the Python client's summary JSON receive
path. It does not replace scheduling, since/detail processing, or the public
`QueryNode` / `QuerySnapshot` interfaces. The main `takler` wheel remains pure
Python and does not require Rust or this package.

Build a wheel for the target CPython (3.11 or newer) and platform, then install
it alongside the corresponding takler checkout/release:

```sh
python -m pip wheel --no-deps ./native/summary -w dist
python -m pip install dist/takler_summary_kernel-*.whl
```

Building from source requires Cargo/Rust with edition 2024 support. The build
backend and Cargo dependency graph are pinned. Wheels are interpreter/platform
specific; a wheel built on CPython 3.14 Linux is not a universal artifact.
The extension is optional: when absent, incompatible with this client API, or
unloadable (`ImportError`/`OSError`), the original Python receiver is used.
Uninstall `takler-summary-kernel` to return to that path. Input rejection is
different: the original strict decoder must validate any native fallback;
invalid input never becomes accepted because the extension rejected it.

Every actual node is validated in Rust, including strict types, duplicate and
escaped keys, Unicode, paths, generation membership, page size, and cross-page
scope/parent/depth/budget rules. Python retains header, page order, identity,
total count, cancellation and atomic publication checks. The header passed to
Python contains a root witness only for the nonempty-page rule; this witness
does not validate or replace the actual nodes. All final Python nodes are built
inside the receive operation using the original constructor.

HTTP and gRPC use their original metadata, retry, timeout and error paths.
Custom transports may opt in with `query_summary(request, decode)`: invoke
`decode` exactly once on successful response bytes at the normal decode point,
return its private result unchanged, and preserve all existing archive and
deadline handling. Transports without this method use the original path.
Replacing a built-in transport's `query` disables its inherited native entry;
an extension must explicitly provide a matching callback to opt in. Registered
page models and custom node constructors/from-wire methods use Python.

Run the regular client/protocol tests without the extension, then with the
built wheel installed. `tests/client/test_summary_native.py` holds the native
differential and boundary cases; it skips when the optional wheel is absent.
`tests/client/test_summary_transport.py` exercises both transport boundaries.
No test in those files is a benchmark or opens a 100k archive.

The first candidate reuses the previously evaluated kernel without GIL changes,
worker pools, deferred conversion or shared client views. It still requires
real-load validation; an installed native module is not a capacity guarantee.
