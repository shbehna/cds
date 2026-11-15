"""Tests of the Pulumi program with the Pulumi mocks: no Pulumi CLI nor workspace is needed."""

from __future__ import annotations

import asyncio
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
        # A failed service fails the run, and the services after it do not run.
        assert all(value(task, "run_if") is None for task in tasks)
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


DYNAMIC = "pulumi-python:dynamic:Resource"


@pulumi.runtime.test
def test_masking(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    masked = dataclasses.replace(
        settings, masking=True, create_tag_policies=True, masking_exempt_principals=("data-stewards",)
    )
    job = pipeline.deploy(masked)

    def check(_: str) -> None:
        [tag_policy] = created("databricks:index/tagPolicy:TagPolicy")
        assert tag_policy.inputs["tagKey"] == "classification"
        # The policies wait for the new governed tag to propagate.
        [delay] = created(DYNAMIC, "tag-policy-propagation:classification")
        assert delay.inputs["seconds"] == pipeline.TAG_POLICY_PROPAGATION_SECONDS
        assert delay.inputs["trigger"] == "tag-policy:classification-id"
        names = [value(item, "name") for item in tag_policy.inputs["values"]]
        assert names == ["public", "internal", "confidential", "restricted"]
        [schema] = created("databricks:index/schema:Schema")
        assert (schema.inputs["catalogName"], schema.inputs["name"]) == ("nprod", "governance")
        [function] = created(DYNAMIC, "mask-function")
        assert value(function.inputs, "object_name") == "nprod.governance.mask"
        assert function.inputs["create"].startswith(
            "CREATE OR REPLACE FUNCTION `nprod`.`governance`.`mask`(value STRING) RETURNS STRING"
        )
        assert function.inputs["delete"] == "DROP FUNCTION IF EXISTS `nprod`.`governance`.`mask`"
        assert value(function.inputs, "warehouse_id") == "warehouse-of-Serverless Starter Warehouse"
        assert value(function.inputs, "client_id") == "bf175dc4-c86b-4e88-83e9-9194c485a080"
        [policy] = created("databricks:index/policyInfo:PolicyInfo")
        inputs = policy.inputs
        assert inputs["name"] == "mask_restricted"
        assert (inputs["onSecurableType"], inputs["onSecurableFullname"]) == ("CATALOG", "nprod")
        assert (inputs["forSecurableType"], inputs["policyType"]) == ("TABLE", "POLICY_TYPE_COLUMN_MASK")
        assert inputs["toPrincipals"] == ["account users"]
        # The service principal running the job reads the data as it is.
        assert inputs["exceptPrincipals"] == ["bf175dc4-c86b-4e88-83e9-9194c485a080", "data-stewards"]
        [match] = inputs["matchColumns"]
        assert value(match, "condition") == "has_tag_value('classification', 'restricted')"
        mask = inputs["columnMask"]
        assert value(mask, "function_name") == "nprod.governance.mask"
        assert value(mask, "on_column") == value(match, "alias")

    return job.id.apply(check)


@pulumi.runtime.test
def test_masking_without_creating_the_tag_policy(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    job = pipeline.deploy(dataclasses.replace(settings, masking=True))

    def check(_: str) -> None:
        assert not created("databricks:index/tagPolicy:TagPolicy")
        assert not created(DYNAMIC, "tag-policy-propagation")
        assert len(created("databricks:index/policyInfo:PolicyInfo")) == 1

    return job.id.apply(check)


@pulumi.runtime.test
def test_no_masking_by_default(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    job = pipeline.deploy(settings)

    def check(_: str) -> None:
        assert not created("databricks:index/policyInfo:PolicyInfo")
        assert not created("databricks:index/schema:Schema")
        assert not created(DYNAMIC)

    return job.id.apply(check)


@pulumi.runtime.test
def test_masking_nothing(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    job = pipeline.deploy(dataclasses.replace(settings, masking=True, masked_classifications=()))

    def check(_: str) -> None:
        assert not created("databricks:index/policyInfo:PolicyInfo")
        assert created(DYNAMIC, "mask-function")

    return job.id.apply(check)


@pulumi.runtime.test
def test_the_tag_policy_is_protected(settings: pipeline.Settings) -> None:
    """Destroying the stack creating the tag policy must not remove the masking of the other stacks."""
    [tag_policy, _] = pipeline.governance(
        dataclasses.replace(settings, masking=True, create_tag_policies=True), "w", None
    )

    assert isinstance(tag_policy, pulumi.CustomResource) and tag_policy._protect


def test_string_list() -> None:
    assert pipeline.string_list("k", None, ("default",)) == ("default",)
    assert pipeline.string_list("k", [], ("default",)) == ()
    assert pipeline.string_list("k", ["a", "b"]) == ("a", "b")
    for invalid in ("restricted", ["a", 1], {"a": "b"}):
        with pytest.raises(ValueError, match="cds-demo:k must be a list of strings"):
            pipeline.string_list("k", invalid)


def test_masking_errors(settings: pipeline.Settings, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="needs the service principal"):
        pipeline.governance(dataclasses.replace(settings, masking=True, service_principal=None), "w", None)
    with pytest.raises(ValueError, match="Unknown masked classification"):
        pipeline.governance(dataclasses.replace(settings, masked_classifications=("secret",)), "w", None)
    with pytest.raises(FileNotFoundError, match=r"manifest\.py"):
        pipeline.manifest_classification(tmp_path / "req_manifest.json")


def test_the_stacks_mask_the_classified_columns() -> None:
    def config(stack: str) -> dict[str, str]:
        text = (pipeline.REPOSITORY / "cds-demo" / "deploy" / f"Pulumi.{stack}.yaml").read_text(encoding="utf-8")
        values = re.findall(r"^  cds-demo:(\w+): *(.+)$", text, flags=re.MULTILINE)
        return {key: raw.strip().strip('"') for key, raw in values}

    nprod, prod = config("nprod"), config("prod")
    assert nprod["masking"] == prod["masking"] == "true"
    # The tag policy belongs to the account: the nprod stack creates it, protected, for both.
    assert (nprod["createTagPolicies"], prod["createTagPolicies"]) == ("true", "false")


@pulumi.runtime.test
def test_the_job_runs_on_the_frequency_of_the_contracts(settings: pipeline.Settings) -> pulumi.Output[None]:
    paused = pipeline.deploy(dataclasses.replace(settings, pause_schedule=True))
    running = pipeline.deploy(dataclasses.replace(settings, timezone="UTC"))

    def check(schedules: list[Any]) -> None:
        paused_schedule, running_schedule = schedules
        # The register is published twice a month: the 1st and the 15th, at 6 am.
        assert value(paused_schedule, "quartz_cron_expression") == "0 0 6 1,15 * ?"
        assert (value(paused_schedule, "timezone_id"), value(paused_schedule, "pause_status")) == (
            "America/Toronto",
            "PAUSED",
        )
        assert (value(running_schedule, "timezone_id"), value(running_schedule, "pause_status")) == ("UTC", "UNPAUSED")

    return pulumi.Output.all(paused.schedule, running.schedule).apply(check)


@pulumi.runtime.test
def test_alerts(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    job = pipeline.deploy(dataclasses.replace(settings, alert_emails=("data-team@example.org",)))

    def check(_: str) -> None:
        [destination] = created("databricks:index/notificationDestination:NotificationDestination")
        assert destination.inputs["displayName"] == "CDS demo alerts (nprod)"
        email = value(destination.inputs["config"], "email")
        # The email support channel of the contracts, and those of the stack.
        assert value(email, "addresses") == ["donnees-qc-req@example.com", "data-team@example.org"]

        alerts = {args.name: args.inputs for args in created("databricks:index/alertV2:AlertV2")}
        assert sorted(alerts) == ["alert:error-rate", "alert:latency"]
        for name, inputs in alerts.items():
            assert inputs["parentPath"] == "/Shared/cds-demo/nprod/alerts"
            assert inputs["warehouseId"] == "warehouse-of-Serverless Starter Warehouse"
            assert "${environment}" not in inputs["queryText"], name
            assert value(inputs["runAs"], "service_principal_name") == "bf175dc4-c86b-4e88-83e9-9194c485a080"
            evaluation = inputs["evaluation"]
            source = value(evaluation, "source")
            assert (value(source, "name"), value(source, "aggregation")) == ("exceeded", "MAX")
            assert value(evaluation, "comparison_operator") == "GREATER_THAN"
            assert value(value(value(evaluation, "threshold"), "value"), "double_value") == 0
            [subscription] = value(value(evaluation, "notification"), "subscriptions")
            assert value(subscription, "destination_id") == "alert-destination-id"
            schedule = inputs["schedule"]
            assert value(schedule, "quartz_cron_schedule") == "0 0 7 * * ?"
            assert (value(schedule, "timezone_id"), value(schedule, "pause_status")) == ("America/Toronto", "UNPAUSED")
        assert alerts["alert:error-rate"]["displayName"] == "REQ error rate SLA (nprod)"
        assert "FROM `nprod`.`donnees_qc_req`.`entreprise`" in alerts["alert:error-rate"]["queryText"]
        assert "FROM `nprod`.`information_schema`.`tables`" in alerts["alert:latency"]["queryText"]
        directories = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/directory:Directory"}
        assert "/Shared/cds-demo/nprod/alerts" in directories

    return job.id.apply(check)


@pulumi.runtime.test
def test_alerts_without_recipients(settings: pipeline.Settings, tmp_path: Path) -> pulumi.Output[None]:
    CREATED.clear()
    manifest = json.loads(pipeline.MANIFEST.read_text(encoding="utf-8"))
    (tmp_path / "manifest.json").write_text(json.dumps({**manifest, "recipients": []}), encoding="utf-8")
    job = pipeline.deploy(dataclasses.replace(settings, manifest=tmp_path / "manifest.json"))

    def check(_: str) -> None:
        assert not created("databricks:index/notificationDestination:NotificationDestination")
        alerts = created("databricks:index/alertV2:AlertV2")
        assert len(alerts) == 2
        assert all(value(args.inputs["evaluation"], "notification") is None for args in alerts)

    return job.id.apply(check)


@pulumi.runtime.test
def test_no_schedule_nor_alerts_without_sla(settings: pipeline.Settings, tmp_path: Path) -> pulumi.Output[None]:
    CREATED.clear()
    manifest = json.loads(pipeline.MANIFEST.read_text(encoding="utf-8"))
    (tmp_path / "manifest.json").write_text(json.dumps({**manifest, "schedule": None, "alerts": []}), encoding="utf-8")
    job = pipeline.deploy(dataclasses.replace(settings, manifest=tmp_path / "manifest.json"))

    def check(schedule: Any) -> None:
        assert schedule is None
        assert not created("databricks:index/alertV2:AlertV2")
        assert not created("databricks:index/notificationDestination:NotificationDestination")
        directories = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/directory:Directory"}
        assert "/Shared/cds-demo/nprod/alerts" not in directories

    return job.schedule.apply(check)


def test_environment_query() -> None:
    assert pipeline.environment_query("SELECT * FROM `${environment}`.`s`.`t`", "nprod") == (
        "SELECT * FROM `nprod`.`s`.`t`"
    )
    with pytest.raises(ValueError, match="Invalid environment"):
        pipeline.environment_query("SELECT 1", "n'prod")


def test_the_stacks_pause_the_schedule_in_nprod_only() -> None:
    def config(stack: str) -> dict[str, str]:
        text = (pipeline.REPOSITORY / "cds-demo" / "deploy" / f"Pulumi.{stack}.yaml").read_text(encoding="utf-8")
        values = re.findall(r"^  cds-demo:(\w+): *(.+)$", text, flags=re.MULTILINE)
        return {key: raw.strip().strip('"') for key, raw in values}

    nprod, prod = config("nprod"), config("prod")
    assert (nprod["pauseSchedule"], prod["pauseSchedule"]) == ("true", "false")
    assert nprod["timezone"] == prod["timezone"] == "America/Toronto"


def test_the_alert_emails_are_a_secret_of_the_environment() -> None:
    """The workflow sets ALERT_EMAILS from the GitHub secret: the addresses stay secret in Pulumi."""
    emails = pipeline.alert_emails_from_environment({"ALERT_EMAILS": " me@example.org, team@example.org ;"})
    assert emails is not None
    addresses = pipeline.alert_addresses(["support@example.com"], emails)

    assert isinstance(addresses, pulumi.Output)
    loop = asyncio.get_event_loop()
    assert loop.run_until_complete(addresses.is_secret())
    assert loop.run_until_complete(addresses.future()) == ["support@example.com", "me@example.org", "team@example.org"]
    # Without the secret, the recipients of the manifest, as a plain list.
    assert pipeline.alert_emails_from_environment({}) is None
    assert pipeline.alert_emails_from_environment({"ALERT_EMAILS": ""}) is None
    assert pipeline.alert_addresses(["support@example.com"], None) == ["support@example.com"]


def test_invalid_alert_emails_are_not_shown() -> None:
    with pytest.raises(ValueError, match="the value number 2 is not an email address") as raised:
        pipeline.alert_emails_from_environment({"ALERT_EMAILS": "me@example.org,secret-name"})

    assert "secret-name" not in str(raised.value) and "me@example.org" not in str(raised.value)
