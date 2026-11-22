"""Runs the jobs of the data products of a deployment, and fails if one of them fails.

The deployment workflow (.github/workflows/deploy-demo.yml) runs it after ``pulumi up``,
so that a deployment whose data pipeline fails is a failed deployment:

    python run_jobs.py [--data-product donnees_qc.req ...] [--timeout-minutes 10]

It reads the ids of the jobs, by data product, in the ``JOB_IDS`` environment variable: the
``job_ids`` output of the stack, as JSON. The Databricks SDK authenticates as the
deployment service principal, with the ``DATABRICKS_HOST``, ``DATABRICKS_CLIENT_ID`` and
``DATABRICKS_CLIENT_SECRET`` environment variables.

It runs the jobs of the data products given by ``--data-product``, or of all of them
without it. The jobs run one after the other: Databricks Free Edition refuses a run while 5 tasks are
active, and each job has 5 tasks. Each run has at most ``--timeout-minutes`` to finish: a
run still going is then canceled, and counts as failed. The script runs every job, even
after a failed one, then exits with an error if one of them did not succeed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Protocol

from databricks.sdk.service.jobs import Run, RunLifeCycleState, RunResultState

JOB_IDS_VARIABLE = "JOB_IDS"
DEFAULT_TIMEOUT_MINUTES = 10
# Seconds between two checks of the state of a run.
POLL_SECONDS = 15
# The life cycle states of a finished run.
FINISHED = (RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR)


class RunNowWait(Protocol):
    """What the script reads of the ``Wait`` returned by ``JobsAPI.run_now``."""

    def bind(self) -> dict[str, object]: ...


class Jobs(Protocol):
    """The part of ``JobsAPI`` of the Databricks SDK that the script uses."""

    def run_now(self, job_id: int) -> RunNowWait: ...

    def get_run(self, run_id: int) -> Run: ...

    def cancel_run(self, run_id: int) -> object: ...


def job_ids(value: str | None) -> dict[str, int]:
    """The ids of the jobs by data product, from the JSON of the ``job_ids`` output of the stack."""
    if not value:
        raise ValueError(f"{JOB_IDS_VARIABLE} is not set: it holds the job_ids output of the stack, as JSON.")
    ids = json.loads(value)
    if not isinstance(ids, dict) or not ids:
        raise ValueError(f"{JOB_IDS_VARIABLE} is not a JSON object of job ids by data product: {value}")
    return {str(data_product): int(job_id) for data_product, job_id in ids.items()}


def outcome(run: Run) -> str | None:
    """Why a finished run did not succeed, or None when it succeeded."""
    state = run.state
    if state and state.result_state == RunResultState.SUCCESS:
        return None
    result = state.result_state.value if state and state.result_state else None
    life_cycle = state.life_cycle_state.value if state and state.life_cycle_state else "unknown"
    reasons = [result or life_cycle]
    if state and state.state_message:
        reasons.append(state.state_message)
    # The tasks that did not succeed, with their message: the cause is in the first one.
    for task in run.tasks or []:
        task_state = task.state
        if task_state and task_state.result_state and task_state.result_state != RunResultState.SUCCESS:
            message = f": {task_state.state_message}" if task_state.state_message else ""
            reasons.append(f"task {task.task_key} {task_state.result_state.value}{message}")
    return "; ".join(reasons)


def run_job(
    jobs: Jobs,
    data_product: str,
    job_id: int,
    timeout_seconds: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> str | None:
    """Runs a job and waits for its end: returns why it did not succeed, or None when it succeeded.

    A run that has not finished within timeout_seconds is canceled. So is a run whose wait
    is interrupted, as when the workflow is canceled: it does not keep running unwatched.
    """
    run_id = int(str(jobs.run_now(job_id=job_id).bind()["run_id"]))
    deadline = clock() + timeout_seconds
    finished = False
    try:
        while True:
            run = jobs.get_run(run_id=run_id)
            state = run.state
            if state and state.life_cycle_state in FINISHED:
                finished = True
                failure = outcome(run)
                print(f"{data_product}: {failure or 'SUCCESS'} ({run.run_page_url})", flush=True)
                return failure
            if clock() >= deadline:
                return f"not finished within {timeout_seconds:.0f} seconds, canceled"
            life_cycle = state.life_cycle_state.value if state and state.life_cycle_state else "unknown"
            print(f"{data_product}: {life_cycle} ({run.run_page_url})", flush=True)
            sleep(POLL_SECONDS)
    finally:
        if not finished:
            jobs.cancel_run(run_id=run_id)


def selected(ids: Mapping[str, int], data_products: Sequence[str] | None) -> dict[str, int]:
    """The jobs of the given data products, in their order, or all the jobs when none is given."""
    if not data_products:
        return dict(ids)
    unknown = [data_product for data_product in data_products if data_product not in ids]
    if unknown:
        raise ValueError(f"No job for the data products {', '.join(unknown)}: the jobs are those of {', '.join(ids)}.")
    return {data_product: ids[data_product] for data_product in data_products}


def run_jobs(
    jobs: Jobs,
    ids: Mapping[str, int],
    timeout_seconds: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, str]:
    """Runs the jobs one after the other: returns why each failed job failed, by data product."""
    failures: dict[str, str] = {}
    for data_product, job_id in ids.items():
        print(f"{data_product}: running the job {job_id}", flush=True)
        failure = run_job(jobs, data_product, job_id, timeout_seconds, sleep=sleep, clock=clock)
        if failure:
            failures[data_product] = failure
    return failures


def main(argv: Sequence[str] | None = None, jobs: Jobs | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument(
        "--data-product",
        action="append",
        dest="data_products",
        help="Qualified name of a data product whose job runs, such as donnees_qc.req; repeatable (default: all).",
    )
    parser.add_argument(
        "--timeout-minutes",
        type=float,
        default=DEFAULT_TIMEOUT_MINUTES,
        help=f"Time each run has to finish before it is canceled (default: {DEFAULT_TIMEOUT_MINUTES}).",
    )
    arguments = parser.parse_args(argv)
    ids = selected(job_ids(os.environ.get(JOB_IDS_VARIABLE)), arguments.data_products)
    if jobs is None:
        from databricks.sdk import WorkspaceClient

        jobs = WorkspaceClient().jobs
    failures = run_jobs(jobs, ids, arguments.timeout_minutes * 60)
    for data_product, failure in failures.items():
        # An error annotation of GitHub Actions, shown in the summary of the workflow run.
        print(f"::error::The job of {data_product} failed: {failure}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
