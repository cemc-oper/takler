"""Check the exact tagged source, its CI run, and release artifact hashes.

The JSON file options on ``source`` make the GitHub checks reproducible without
network access. They are only accepted together and are never used by CI.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import quote
from urllib.request import Request, urlopen


SHA = re.compile(r"[0-9a-f]{40}\Z")
TAG = re.compile(r"v[0-9][0-9A-Za-z.+-]*\Z")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")


class GateError(Exception):
    pass


def git(checkout: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(checkout), *args], capture_output=True, text=True, check=False
    )
    if result.returncode:
        raise GateError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def source_commit(checkout: Path, tag: str, expected_sha: str | None = None) -> str:
    if not TAG.fullmatch(tag):
        raise GateError("tag must be a simple v-prefixed version")
    head = git(checkout, "rev-parse", "HEAD")
    tagged = git(checkout, "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}")
    if head != tagged:
        raise GateError(f"checkout {head} does not match {tag} commit {tagged}")
    if expected_sha and (not SHA.fullmatch(expected_sha) or head != expected_sha):
        raise GateError("event commit does not match the tagged checkout")
    return head


def check_remote_tag(checkout: Path, tag: str, sha: str) -> None:
    remote = git(
        checkout,
        "ls-remote",
        "--tags",
        "origin",
        f"refs/tags/{tag}",
        f"refs/tags/{tag}^{{}}",
    )
    refs = dict(line.split("\t", 1)[::-1] for line in remote.splitlines())
    remote_sha = refs.get(f"refs/tags/{tag}^{{}}") or refs.get(f"refs/tags/{tag}")
    if remote_sha != sha:
        raise GateError("remote tag no longer points to the verified commit")


def api_json(repository: str, path: str) -> dict:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise GateError("GITHUB_TOKEN is required for CI verification")
    request = Request(
        f"https://api.github.com/repos/{repository}/{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urlopen(request, timeout=20) as response:
        return json.load(response)


def select_ci_run(runs: dict, sha: str) -> dict:
    if runs.get("total_count", 0) > 100:
        raise GateError("too many CI runs for one commit; inspect manually")
    matches = [
        run
        for run in runs.get("workflow_runs", [])
        if run.get("head_sha") == sha
        and run.get("event") == "push"
        and run.get("head_branch") == "main"
    ]
    if not matches:
        raise GateError(f"no main push CI run for tagged commit {sha}")
    latest = max(matches, key=lambda run: run["id"])
    if latest.get("status") != "completed" or latest.get("conclusion") != "success":
        raise GateError(f"latest CI run {latest['id']} did not succeed")
    return latest


def check_jobs(jobs: dict, required: list[str], sha: str) -> None:
    if jobs.get("total_count", 0) > 100:
        raise GateError("too many CI jobs; inspect manually")
    found = {job.get("name"): job for job in jobs.get("jobs", [])}
    for name in required:
        job = found.get(name)
        if (
            not job
            or job.get("status") != "completed"
            or job.get("conclusion") != "success"
        ):
            raise GateError(f"required CI job {name!r} did not succeed")
        if job.get("head_sha") not in (None, sha):
            raise GateError(f"required CI job {name!r} belongs to another commit")


def source(args: argparse.Namespace) -> None:
    if not REPOSITORY.fullmatch(args.repository):
        raise GateError("repository must be owner/name")
    if not args.required_job:
        raise GateError("at least one required CI job is needed")
    if bool(args.runs_file) != bool(args.jobs_file):
        raise GateError("offline runs and jobs files must be supplied together")
    sha = source_commit(args.checkout, args.tag, args.expected_sha)
    if args.check_remote:
        check_remote_tag(args.checkout, args.tag, sha)
    if args.runs_file:
        runs = json.loads(args.runs_file.read_text())
    else:
        runs = api_json(
            args.repository,
            f"actions/workflows/ci.yml/runs?head_sha={quote(sha)}&per_page=100",
        )
    run = select_ci_run(runs, sha)
    if args.jobs_file:
        jobs = json.loads(args.jobs_file.read_text())
    else:
        jobs = api_json(
            args.repository, f"actions/runs/{run['id']}/jobs?filter=latest&per_page=100"
        )
    check_jobs(jobs, args.required_job, sha)
    proof = {
        "repository": args.repository,
        "tag": args.tag,
        "commit": sha,
        "ci_run_id": run["id"],
        "required_jobs": args.required_job,
    }
    args.proof_file.write_text(json.dumps(proof, sort_keys=True, indent=2) + "\n")
    print(f"release source: {args.tag} {sha}; CI run {run['id']} passed")


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def artifact_files(directory: Path, manifest: Path) -> list[Path]:
    files = sorted(
        path for path in directory.iterdir() if path.is_file() and path != manifest
    )
    if not files:
        raise GateError("release has no artifacts")
    return files


def manifest(args: argparse.Namespace) -> None:
    proof = json.loads(args.proof_file.read_text())
    files = artifact_files(args.dist, args.manifest)
    proof["artifacts"] = {path.name: digest(path) for path in files}
    args.manifest.write_text(json.dumps(proof, sort_keys=True, indent=2) + "\n")
    print(f"release manifest: {len(files)} artifact(s) for {proof['commit']}")


def verify(args: argparse.Namespace) -> None:
    data = json.loads(args.manifest.read_text())
    if data.get("repository") != args.repository or data.get("tag") != args.tag:
        raise GateError("artifact manifest belongs to another repository or tag")
    sha = source_commit(args.checkout, args.tag, args.expected_sha)
    if args.check_remote:
        check_remote_tag(args.checkout, args.tag, sha)
    if data.get("commit") != sha:
        raise GateError("artifact manifest commit differs from tagged checkout")
    actual = {
        path.name: digest(path) for path in artifact_files(args.dist, args.manifest)
    }
    if actual != data.get("artifacts"):
        raise GateError("artifact names or SHA-256 checksums do not match the manifest")
    print(f"release artifacts: verified {len(actual)} file(s) for {sha}")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    source_command = commands.add_parser("source")
    source_command.add_argument("--checkout", type=Path, default=Path("."))
    source_command.add_argument("--tag", required=True)
    source_command.add_argument("--repository", required=True)
    source_command.add_argument("--expected-sha")
    source_command.add_argument("--check-remote", action="store_true")
    source_command.add_argument("--required-job", action="append", default=[])
    source_command.add_argument("--runs-file", type=Path)
    source_command.add_argument("--jobs-file", type=Path)
    source_command.add_argument("--proof-file", type=Path, required=True)
    source_command.set_defaults(handler=source)
    manifest_command = commands.add_parser("manifest")
    manifest_command.add_argument("--proof-file", type=Path, required=True)
    manifest_command.add_argument("--dist", type=Path, required=True)
    manifest_command.add_argument("--manifest", type=Path, required=True)
    manifest_command.set_defaults(handler=manifest)
    verify_command = commands.add_parser("verify")
    verify_command.add_argument("--checkout", type=Path, default=Path("."))
    verify_command.add_argument("--tag", required=True)
    verify_command.add_argument("--repository", required=True)
    verify_command.add_argument("--expected-sha")
    verify_command.add_argument("--check-remote", action="store_true")
    verify_command.add_argument("--dist", type=Path, required=True)
    verify_command.add_argument("--manifest", type=Path, required=True)
    verify_command.set_defaults(handler=verify)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        args.handler(args)
    except (GateError, OSError, ValueError, KeyError) as error:
        print(f"release gate: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
