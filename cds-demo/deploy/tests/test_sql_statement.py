"""Tests of the SqlStatement provider, without workspace: the Databricks SDK client is replaced."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from databricks.sdk.service.sql import StatementState
from pulumi.dynamic.dynamic import serialize_provider

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import sql_statement

PROPS = {
    "name": "mask-function",
    "warehouse_id": "w1",
    "object_name": "main.governance.f",
    "create": "CREATE OR REPLACE FUNCTION f() RETURNS INT RETURN 1",
    "delete": "DROP FUNCTION IF EXISTS f",
    "host": "https://workspace",
    "client_id": "sp",
}


class FakeStatements:
    """The statement execution API of the SDK: the statements go through the given states."""

    def __init__(self) -> None:
        self.states: list[StatementState] = []
        self.error: str | None = None
        self.executed: list[tuple[str, str]] = []
        self.clients: list[dict[str, Any]] = []
        self.canceled: list[str] = []

    def client(self, **kwargs: Any) -> SimpleNamespace:
        self.clients.append(kwargs)
        return SimpleNamespace(statement_execution=self)

    def _response(self) -> SimpleNamespace:
        state = self.states.pop(0) if self.states else StatementState.SUCCEEDED
        error = SimpleNamespace(message=self.error) if self.error else None
        return SimpleNamespace(statement_id="s1", status=SimpleNamespace(state=state, error=error))

    def execute_statement(self, *, statement: str, warehouse_id: str, wait_timeout: str) -> SimpleNamespace:
        self.executed.append((warehouse_id, statement))
        return self._response()

    def get_statement(self, statement_id: str) -> SimpleNamespace:
        return self._response()

    def cancel_execution(self, statement_id: str) -> None:
        self.canceled.append(statement_id)


@pytest.fixture
def statements(monkeypatch: pytest.MonkeyPatch) -> FakeStatements:
    fake = FakeStatements()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", fake.client)
    monkeypatch.setattr("time.sleep", lambda _: None)
    return fake


def test_create_update_and_delete_run_their_statements(statements: FakeStatements) -> None:
    provider = sql_statement.statement_provider()
    updated = {**PROPS, "create": "CREATE OR REPLACE FUNCTION f() RETURNS INT RETURN 2"}

    result = provider.create(dict(PROPS))
    provider.update("main.governance.f", dict(PROPS), updated)
    provider.delete("main.governance.f", dict(PROPS))

    assert result.id == "main.governance.f"
    assert statements.executed == [("w1", PROPS["create"]), ("w1", updated["create"]), ("w1", PROPS["delete"])]
    assert statements.clients[0] == {"host": "https://workspace", "client_id": "sp"}


def test_an_update_of_the_other_inputs_runs_nothing(statements: FakeStatements) -> None:
    updated = {**PROPS, "delete": "DROP FUNCTION f", "warehouse_id": "w2"}

    result = sql_statement.statement_provider().update("main.governance.f", dict(PROPS), updated)

    assert statements.executed == []
    assert result.outs == updated


def test_waits_for_the_statement(statements: FakeStatements) -> None:
    statements.states = [StatementState.PENDING, StatementState.RUNNING, StatementState.SUCCEEDED]

    sql_statement.statement_provider().create(dict(PROPS))

    assert statements.states == []


def test_a_failed_statement_raises(statements: FakeStatements) -> None:
    statements.states = [StatementState.FAILED]
    statements.error = "PERMISSION_DENIED"

    with pytest.raises(RuntimeError, match=r"FAILED.*PERMISSION_DENIED"):
        sql_statement.statement_provider().create(dict(PROPS))


def test_a_statement_still_running_is_canceled(statements: FakeStatements, monkeypatch: pytest.MonkeyPatch) -> None:
    statements.states = [StatementState.RUNNING] * 3
    clock = iter([0.0, 300.0, 601.0])
    monkeypatch.setattr("time.monotonic", lambda: next(clock))

    with pytest.raises(RuntimeError, match="did not finish within 600 seconds"):
        sql_statement.statement_provider().create(dict(PROPS))

    assert statements.canceled == ["s1"]


def test_diff() -> None:
    provider = sql_statement.statement_provider()

    assert not provider.diff("id", dict(PROPS), dict(PROPS)).changes
    updated = provider.diff("id", dict(PROPS), {**PROPS, "create": "CREATE OR REPLACE FUNCTION f() RETURN 2"})
    assert updated.changes and not updated.replaces
    renamed = provider.diff("id", dict(PROPS), {**PROPS, "object_name": "main.governance.g"})
    # The new object is created before the previous one is dropped.
    assert renamed.changes and renamed.replaces == ["object_name"] and not renamed.delete_before_replace


def test_the_provider_runs_without_its_module(tmp_path: Path) -> None:
    """Pulumi runs the provider in another process, where the module may not be importable."""
    serialized = serialize_provider(sql_statement.statement_provider())
    script = "\n".join(
        [
            "import base64, sys, dill",
            "provider = dill.loads(base64.b64decode(sys.stdin.read()))",
            "assert 'sql_statement' not in sys.modules",
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
    assert result.stdout.strip() == "SqlStatementProvider"
