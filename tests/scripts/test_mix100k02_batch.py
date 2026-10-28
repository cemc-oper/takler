"""Batch identity and frozen-source regression; no product services."""

import hashlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import mix100k01 as mix


def test_new_batch_freezes_current_sources_without_old_budget(tmp_path):
    repo = Path(__file__).resolve().parents[2]
    mix.freeze(tmp_path, repo, "MIX100K-02")
    config = mix.read(tmp_path / "config.json")
    assert config["batch"] == "MIX100K-02"
    assert config["order"] == ["smoke", "D", "B", "C", "A"]
    assert config["slots"] == 18000 and config["smoke"]["slots"] == 600
    assert not (tmp_path / "budget.json").exists()
    assert (tmp_path / "config.sha256").read_text().strip() == hashlib.sha256(
        (tmp_path / "config.json").read_bytes()
    ).hexdigest()
    source = tmp_path / "diagnostic"
    for name, digest in mix.read(source / "source-index.json").items():
        assert hashlib.sha256((source / name).read_bytes()).hexdigest() == digest
    assert (source / "scripts/mix100k01.py").read_bytes() == (
        repo / "scripts/mix100k01.py"
    ).read_bytes()
    assert (source / "gc.patch").stat().st_size > 0
    with pytest.raises(ValueError, match="already frozen"):
        mix.freeze(tmp_path, repo, "MIX100K-02")
