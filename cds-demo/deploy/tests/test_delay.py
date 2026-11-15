"""Tests of the Delay provider, without waiting: the sleep is replaced."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from pulumi.dynamic.dynamic import serialize_provider

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import delay

PROPS = {"name": "tag-policy-propagation:classification", "seconds": 120, "trigger": "classification"}


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr("time.sleep", slept.append)
    return slept


def test_create_waits(sleeps: list[float]) -> None:
    result = delay.delay_provider().create(dict(PROPS))

    assert sleeps == [120.0]
    assert result.id == "tag-policy-propagation:classification"


def test_update_and_delete_do_not_wait(sleeps: list[float]) -> None:
    provider = delay.delay_provider()

    provider.update("id", dict(PROPS), {**PROPS, "seconds": 60})
    provider.delete("id", dict(PROPS))

    assert sleeps == []


def test_diff() -> None:
    provider = delay.delay_provider()

    assert not provider.diff("id", dict(PROPS), dict(PROPS)).changes
    # A new trigger replaces the resource, which waits again.
    triggered = provider.diff("id", dict(PROPS), {**PROPS, "trigger": "another"})
    assert triggered.changes and triggered.replaces == ["trigger"]
    longer = provider.diff("id", dict(PROPS), {**PROPS, "seconds": 300})
    assert longer.changes and not longer.replaces


def test_the_provider_runs_without_its_module(tmp_path: Path) -> None:
    """Pulumi runs the provider in another process, where the module may not be importable."""
    serialized = serialize_provider(delay.delay_provider())
    script = "\n".join(
        [
            "import base64, sys, dill",
            "provider = dill.loads(base64.b64decode(sys.stdin.read()))",
            "assert 'delay' not in sys.modules",
            "print(type(provider).__name__)",
        ]
    )

    result = subprocess.run(
        [sys.executable, "-I", "-c", script],
        input=serialized,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "DelayProvider"
