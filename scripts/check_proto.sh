#!/usr/bin/env bash
#
# Proto drift and freshness gate for the takler repo.
#
# src/takler/server/protocol/takler.proto is the single source of truth of
# the wire protocol. Two things can silently drift away from it, and this
# script fails on either:
#
#   1. freshness: the checked in Python stubs (takler_pb2.py,
#      takler_pb2_grpc.py) differ from a fresh regeneration with the locked
#      grpcio-tools — someone edited the proto without regenerating;
#   2. cross repo drift: the takler-client repo's takler_protocol/takler.proto
#      copy differs from the source of truth — the Go side did not catch up.
#
# The Go side runs the mirror image of this gate (takler-client's
# scripts/proto.sh check); selecting an incompatible peer makes the paired
# CI gate fail. CI selects peer main or an explicit commit SHA.
#
# The peer must be an explicit local checkout (default ../takler-client).
# CI checks out the selected peer first; do not fetch a separate proto URL.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

proto_rel="takler/server/protocol/takler.proto"

gen_dir="$(mktemp -d)"
trap 'rm -rf "${gen_dir}"' EXIT

failed=0

# 1. freshness of the Python stubs.
uv run --locked python -m grpc_tools.protoc \
    -Isrc --python_out="${gen_dir}" --grpc_python_out="${gen_dir}" \
    "${proto_rel}"
for stub in takler_pb2.py takler_pb2_grpc.py; do
    if ! diff -u "src/takler/server/protocol/${stub}" "${gen_dir}/takler/server/protocol/${stub}"; then
        echo "proto check: src/takler/server/protocol/${stub} is stale; regenerate with:" >&2
        echo "    uv run python -m grpc_tools.protoc -Isrc --python_out=src --grpc_python_out=src ${proto_rel}" >&2
        failed=1
    fi
done

# 2. cross repo drift against takler-client's copy.
client_repo="${TAKLER_CLIENT_REPO:-${repo_root}/../takler-client}"
client_proto="${client_repo}/takler_protocol/takler.proto"
if [[ ! -f "${client_proto}" ]]; then
    echo "proto check: set TAKLER_CLIENT_REPO to the paired checkout" >&2
    exit 1
fi
echo "proto check: takler=$(git rev-parse HEAD) takler-client=$(git -C "${client_repo}" rev-parse HEAD)"
if ! cmp -s "src/${proto_rel}" "${client_proto}"; then
    echo "proto check: src/${proto_rel} differs from takler-client's takler_protocol/takler.proto" >&2
    echo "proto check: catch up in the takler-client repo with \"scripts/proto.sh sync\"" >&2
    failed=1
fi

if [[ "${failed}" -ne 0 ]]; then
    exit 1
fi
echo "proto check: stubs are fresh and takler-client carries the same proto"
