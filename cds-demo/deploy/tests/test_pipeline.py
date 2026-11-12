"""Tests of the Pulumi program with the Pulumi mocks: no Pulumi CLI nor workspace is needed."""

from __future__ import annotations

import dataclasses
import json
import re
import sys
from collections.abc import Sequence
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
        if args.typ == "databricks:index/dashboard:Dashboard":
            outputs["dashboardId"] = "dashboard-1"
        return f"{args.name}-id", outputs

    def call(self, args: pulumi.runtime.MockCallArgs) -> tuple[dict[str, Any], list[tuple[str, str]] | None]:
        if args.token == "databricks:index/getSqlWarehouse:getSqlWarehouse":
            return {"id": f"warehouse-of-{args.args['name']}", "name": args.args["name"]}, []
        return {}, []


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
            "synthetic_data": "false",
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


@pulumi.runtime.test
def test_quality_dashboard(settings: pipeline.Settings) -> pulumi.Output[None]:
    job = pipeline.deploy(settings)

    def check(_: str) -> None:
        # CREATED holds the resources of all the tests: the last dashboard is this one.
        dashboard = [args for args in CREATED if args.typ == "databricks:index/dashboard:Dashboard"][-1]
        inputs = dashboard.inputs
        assert inputs["displayName"] == "REQ data quality (nprod)"
        assert inputs["parentPath"] == "/Shared/cds-demo/nprod/dashboards"
        assert inputs["warehouseId"] == "warehouse-of-Serverless Starter Warehouse"
        assert (inputs["datasetCatalog"], inputs["datasetSchema"]) == ("nprod", "donnees_qc_req")
        assert inputs["embedCredentials"] is True
        content = json.loads(inputs["serializedDashboard"])
        assert [page["displayName"] for page in content["pages"]] == ["Statistics", "Occurrences"]
        directories = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/directory:Directory"}
        assert "/Shared/cds-demo/nprod/dashboards" in directories

    return job.id.apply(check)


def created(typ: str, prefix: str = "") -> list[pulumi.runtime.MockResourceArgs]:
    return [args for args in CREATED if args.typ == typ and args.name.startswith(prefix)]


@pulumi.runtime.test
def test_synthetic_data(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    job = pipeline.deploy(dataclasses.replace(settings, synthetic_data=True))

    def check(parameters: Sequence[Any] | None) -> None:
        uploads = {args.inputs["path"]: args.inputs["source"] for args in created("databricks:index/file:File")}
        names = sorted(path.name for path in pipeline.TESTDATA.glob("*.csv"))
        assert len(names) == 6
        assert sorted(uploads) == [f"/Volumes/ext/donnees_qc/req/{name}" for name in names]
        assert all(Path(source).parent == pipeline.TESTDATA for source in uploads.values())
        values = {value(parameter, "name"): value(parameter, "default") for parameter in parameters or []}
        assert values["synthetic_data"] == "true"

    return job.parameters.apply(check)


@pulumi.runtime.test
def test_no_catalog_nor_synthetic_data_by_default(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    job = pipeline.deploy(settings)

    def check(_: str) -> None:
        assert not created("databricks:index/catalog:Catalog")
        assert not created("databricks:index/grant:Grant")
        assert not created("databricks:index/file:File")

    return job.id.apply(check)


@pulumi.runtime.test
def test_catalog(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    prod = dataclasses.replace(
        settings,
        environment="prod",
        workspace_root="/Shared/cds-demo/prod",
        create_catalog=True,
        catalog_storage_root="s3://bucket/prod",
        catalog_readers=("data-readers",),
    )
    job = pipeline.deploy(prod)

    def check(_: str) -> None:
        [catalog] = created("databricks:index/catalog:Catalog")
        assert catalog.inputs["name"] == "prod"
        assert catalog.inputs["storageRoot"] == "s3://bucket/prod"
        [grant] = created("databricks:index/grant:Grant")
        privileges = pipeline.READER_PRIVILEGES
        assert grant.inputs == {"catalog": "prod", "principal": "data-readers", "privileges": privileges}

    return job.id.apply(check)


def test_the_stacks_load_synthetic_data_in_nprod_only() -> None:
    def config(stack: str) -> dict[str, str]:
        """Returns the values of the cds-demo namespace of a stack file (one per line, quotes removed)."""
        text = (pipeline.REPOSITORY / "cds-demo" / "deploy" / f"Pulumi.{stack}.yaml").read_text(encoding="utf-8")
        return {
            f"cds-demo:{key}": raw.strip().strip('"')
            for key, raw in re.findall(r"^  cds-demo:(\w+): *(.+)$", text, flags=re.MULTILINE)
        }

    nprod, prod = config("nprod"), config("prod")
    assert (nprod["cds-demo:syntheticData"], nprod["cds-demo:createCatalog"]) == ("true", "false")
    # The account uses Default Storage: the catalogs are created in Catalog Explorer.
    assert (prod["cds-demo:syntheticData"], prod["cds-demo:createCatalog"]) == ("false", "false")
    assert nprod["cds-demo:sourcePath"] == prod["cds-demo:sourcePath"]


def test_missing_testdata(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"generate_testdata\.py"):
        pipeline.testdata_files(tmp_path)


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
