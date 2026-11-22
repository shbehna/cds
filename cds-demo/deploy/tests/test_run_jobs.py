"""Tests of run_jobs.py, without workspace: the jobs API of the Databricks SDK is replaced."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from databricks.sdk.service.jobs import Run, RunLifeCycleState, RunResultState, RunState, RunTask

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run_jobs

JOB_IDS = {"donnees_qc.req": 11, "donnees_qc.repertoire_municipalites": 22}


def state(life_cycle: RunLifeCycleState, result: RunResultState | None = None, message: str = "") -> RunState:
    return RunState(life_cycle_state=life_cycle, result_state=result, state_message=message)


RUNNING = state(RunLifeCycleState.RUNNING)
SUCCESS = state(RunLifeCycleState.TERMINATED, RunResultState.SUCCESS)
FAILED = state(RunLifeCycleState.TERMINATED, RunResultState.FAILED, "Task load failed.")


class Waiter:
    """The Wait returned by run_now."""

    def __init__(self, run_id: int) -> None:
        self.run_id = run_id

    def bind(self) -> dict[str, object]:
        return {"run_id": self.run_id}


class FakeJobs:
    """The jobs API: the run of each job goes through the given states, one per get_run."""

    def __init__(self, states: dict[int, list[RunState]], tasks: list[RunTask] | None = None) -> None:
        self.states = states
        self.tasks = tasks
        self.started: list[int] = []
        self.canceled: list[int] = []

    def run_now(self, job_id: int) -> Waiter:
        self.started.append(job_id)
        # A run id per job: 100 times its id.
        return Waiter(job_id * 100)

    def get_run(self, run_id: int) -> Run:
        states = self.states[run_id // 100]
        current = states.pop(0) if len(states) > 1 else states[0]
        return Run(run_id=run_id, state=current, tasks=self.tasks, run_page_url=f"https://workspace/run/{run_id}")

    def cancel_run(self, run_id: int) -> None:
        self.canceled.append(run_id)


class Clock:
    """A clock advanced by the sleeps of the script."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def run(jobs: FakeJobs, timeout_seconds: float = 600) -> dict[str, str]:
    clock = Clock()
    return run_jobs.run_jobs(jobs, JOB_IDS, timeout_seconds, sleep=clock.sleep, clock=clock)


def test_succeeded_jobs() -> None:
    jobs = FakeJobs({11: [RUNNING, RUNNING, SUCCESS], 22: [SUCCESS]})
    assert run(jobs) == {}
    assert jobs.started == [11, 22]
    assert jobs.canceled == []


def test_a_failed_job_fails_and_the_next_one_still_runs() -> None:
    tasks = [
        RunTask(task_key="load", state=state(RunLifeCycleState.TERMINATED, RunResultState.FAILED, "Boom")),
        RunTask(task_key="metadata", state=state(RunLifeCycleState.SKIPPED, RunResultState.UPSTREAM_FAILED)),
        RunTask(task_key="quality", state=SUCCESS),
    ]
    jobs = FakeJobs({11: [RUNNING, FAILED], 22: [SUCCESS]}, tasks)
    assert run(jobs) == {
        "donnees_qc.req": "FAILED; Task load failed.; task load FAILED: Boom; task metadata UPSTREAM_FAILED"
    }
    assert jobs.started == [11, 22]


@pytest.mark.parametrize(
    "final",
    [
        state(RunLifeCycleState.SKIPPED, message="Skipped: the job has reached its maximum of concurrent runs."),
        state(RunLifeCycleState.INTERNAL_ERROR, message="Internal error."),
        state(RunLifeCycleState.TERMINATED, RunResultState.CANCELED),
        state(RunLifeCycleState.TERMINATED, RunResultState.SUCCESS_WITH_FAILURES),
    ],
)
def test_only_a_success_succeeds(final: RunState) -> None:
    failures = run(FakeJobs({11: [final], 22: [SUCCESS]}))
    assert list(failures) == ["donnees_qc.req"]


def test_a_run_past_its_timeout_is_canceled() -> None:
    jobs = FakeJobs({11: [RUNNING], 22: [SUCCESS]})
    assert run(jobs, timeout_seconds=60) == {"donnees_qc.req": "not finished within 60 seconds, canceled"}
    assert jobs.canceled == [1100]
    assert jobs.started == [11, 22]


def test_an_interrupted_wait_cancels_the_run() -> None:
    jobs = FakeJobs({11: [RUNNING]})

    def interrupt(seconds: float) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_jobs.run_job(jobs, "donnees_qc.req", 11, 600, sleep=interrupt)
    assert jobs.canceled == [1100]


def test_job_ids() -> None:
    assert run_jobs.job_ids(json.dumps({"donnees_qc.req": "11"})) == {"donnees_qc.req": 11}
    for value in (None, "", "{}", "[1]"):
        with pytest.raises(ValueError, match="JOB_IDS"):
            run_jobs.job_ids(value)


def test_selected() -> None:
    assert run_jobs.selected(JOB_IDS, None) == JOB_IDS
    assert run_jobs.selected(JOB_IDS, ["donnees_qc.req"]) == {"donnees_qc.req": 11}
    with pytest.raises(ValueError, match=r"donnees_qc\.inconnu"):
        run_jobs.selected(JOB_IDS, ["donnees_qc.req", "donnees_qc.inconnu"])


def test_main(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("JOB_IDS", json.dumps(JOB_IDS))
    monkeypatch.setattr(run_jobs, "POLL_SECONDS", 0)
    assert run_jobs.main([], FakeJobs({11: [SUCCESS], 22: [SUCCESS]})) == 0
    assert run_jobs.main(["--timeout-minutes", "5"], FakeJobs({11: [FAILED], 22: [SUCCESS]})) == 1
    assert "::error::The job of donnees_qc.req failed: FAILED; Task load failed." in capsys.readouterr().out
    jobs = FakeJobs({11: [SUCCESS], 22: [FAILED]})
    assert run_jobs.main(["--data-product", "donnees_qc.req"], jobs) == 0
    assert jobs.started == [11]
