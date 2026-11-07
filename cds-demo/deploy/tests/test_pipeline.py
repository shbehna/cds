"""Tests of the Pulumi program with the Pulumi mocks: no Pulumi CLI nor workspace is needed."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import pulumi
import pytest
import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

CREATED: list[pulumi.runtime.MockResourceArgs] = []


class Mocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs) -> tuple[str | None, dict[str, Any]]:
        CREATED.append(args)
        outputs = dict(args.inputs)
        if args.typ == "databricks:index/job:Job":
            outputs["url"] = "https://workspace/jobs/1"
        return f"{args.name}-id", outputs

    def call(self, args: pulumi.runtime.MockCallArgs) -> tuple[dict[str, Any], list[tuple[str, str]] | None]:
        return {}, None


pulumi.runtime.set_mocks(Mocks(), preview=False)

import pipeline  # noqa: E402


def value(item: object, name: str) -> Any:
    """Reads an attribute of a nested output: a typed object, or a dict with camelCase keys under the mocks."""
    if isinstance(item, dict):
        return item.get(name, item.get(re.sub(r"_([a-z])", lambda match: match.group(1).upper(), name)))
    return getattr(item, name)


@pytest.fixture
def settings(tmp_path: Path) -> pipeline.Settings:
    version = tomllib.loads((pipeline.FRAMEWORK / "pyproject.toml").read_text())["project"]["version"]
    (tmp_path / f"cds_framework-{version}-py3-none-any.whl").write_bytes(b"wheel")
    return pipeline.Settings(
        environment="nprod",
        source_path="/Volumes/ext/donnees_qc/req",
        workspace_root="/Shared/cds-demo/nprod/",
        service_principal="bf175dc4-c86b-4e88-83e9-9194c485a080",
        wheel_directory=tmp_path,
    )


@pulumi.runtime.test
def test_job(settings: pipeline.Settings) -> pulumi.Output[None]:
    job = pipeline.deploy(settings)

    def check(values: list[Any]) -> None:
        name, tasks, environments, parameters, run_as = values
        assert name == "cds-demo-req-nprod"
        dependencies = [
            (value(task, "task_key"), [value(d, "task_key") for d in value(task, "depends_ons") or []])
            for task in tasks
        ]
        assert dependencies == [
            ("schema", []),
            ("metadata", ["schema"]),
            ("transformation", ["schema"]),
            ("quality", ["transformation"]),
            ("scd", ["quality"]),
        ]
        assert {value(value(task, "notebook_task"), "notebook_path") for task in tasks} == {
            f"/Shared/cds-demo/nprod/notebooks/{service}" for service in pipeline.TASKS
        }
        assert all(value(task, "environment_key") == "cds" for task in tasks)
        [environment] = environments
        spec = value(environment, "spec")
        assert value(spec, "environment_version") == "5"
        wheel, dqx = value(spec, "dependencies")
        assert wheel.startswith("/Workspace/Shared/cds-demo/nprod/wheels/")
        assert wheel.endswith("-py3-none-any.whl")
        assert dqx == "databricks-labs-dqx==0.17.0"
        assert {value(parameter, "name"): value(parameter, "default") for parameter in parameters} == {
            "environment": "nprod",
            "roots": "/Workspace/Shared/cds-demo/nprod/products",
            "data_product": "donnees_qc.req",
            "source_path": "/Volumes/ext/donnees_qc/req",
            "max_workers": "4",
        }
        assert value(run_as, "service_principal_name") == "bf175dc4-c86b-4e88-83e9-9194c485a080"

    return pulumi.Output.all(job.name, job.tasks, job.environments, job.parameters, job.run_as).apply(check)


@pulumi.runtime.test
def test_files(settings: pipeline.Settings) -> pulumi.Output[None]:
    job = pipeline.deploy(settings)

    def check(_: str) -> None:
        paths = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/workspaceFile:WorkspaceFile"}
        root = "/Shared/cds-demo/nprod/products/donnees_qc"
        assert f"{root}/donnees_qc_config.py" in paths
        assert f"{root}/req/sources.py" in paths
        assert f"{root}/req/entreprise/entreprise.yaml" in paths
        assert f"{root}/req/entreprise/entreprise_config.py" in paths
        assert not any("__pycache__" in path for path in paths)
        notebooks = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/notebook:Notebook"}
        assert notebooks == {f"/Shared/cds-demo/nprod/notebooks/{service}" for service in pipeline.TASKS}
        directories = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/directory:Directory"}
        assert {path.rsplit("/", 1)[0] for path in paths | notebooks} <= directories
        assert "/Shared/cds-demo/nprod" in directories

    return job.id.apply(check)


def test_folders_are_listed_parents_first() -> None:
    assert pipeline.folders("/r", ["/r/a/b/file", "/r/a/other", "/r/c/file", "/elsewhere/file"]) == [
        "/r",
        "/r/a",
        "/r/c",
        "/r/a/b",
    ]


def test_missing_wheel(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="uv build --package cds-framework"):
        pipeline.framework_wheel(tmp_path)
