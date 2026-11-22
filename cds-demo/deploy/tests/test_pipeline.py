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
        if args.typ == "databricks:index/domain:Domain":
            outputs["domainId"] = f"{args.name}-domain-id"
        return f"{args.name}-id", outputs

    def call(self, args: pulumi.runtime.MockCallArgs) -> tuple[dict[str, Any], list[tuple[str, str]] | None]:
        if args.token == "databricks:index/getSqlWarehouse:getSqlWarehouse":
            return {"id": f"warehouse-of-{args.args['name']}", "name": args.args["name"]}, []
        return {}, []


pulumi.runtime.set_mocks(Mocks(), preview=False)

import pipeline  # noqa: E402
from delay import Delay  # noqa: E402

# What the program reads of the contracts: the tests take their expectations from it, so
# that changing a contract (and regenerating the manifest) does not need changing them.
MANIFEST = pipeline.load_manifest(pipeline.MANIFEST)
DOMAIN_TAGS = [domain["tag"] for domain in MANIFEST["domains"]]
CLASSIFICATION_TAG = MANIFEST["classification"]["tag"]
# The manifests of the data products, by qualified name: a job each.
PRODUCTS = {product["data_product"]: product for product in MANIFEST["data_products"]}


def done(jobs: dict[str, Any]) -> pulumi.Output[list[Any]]:
    """An output resolved once the jobs are created: the checks run then."""
    return pulumi.Output.all(*(job.id for job in jobs.values()))


def with_products(transform: Any) -> dict[str, Any]:
    """The manifest, each data product changed by a function."""
    return {**MANIFEST, "data_products": [transform(product) for product in MANIFEST["data_products"]]}


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
        source_root="/Volumes/ext",
        workspace_root="/Shared/cds-demo/nprod/",
        service_principal="bf175dc4-c86b-4e88-83e9-9194c485a080",
        wheel_directory=tmp_path,
    )


@pulumi.runtime.test
def test_jobs(settings: pipeline.Settings) -> pulumi.Output[None]:
    jobs = pipeline.deploy(settings)
    assert sorted(jobs) == sorted(PRODUCTS)

    def check(values: list[Any]) -> None:
        for data_product, job_values in zip(jobs, values, strict=True):
            check_job(data_product, job_values)

    def check_job(data_product: str, values: list[Any]) -> None:
        name, tasks, environments, parameters, run_as = values
        group, product = data_product.split(".")
        assert name == f"cds-demo-{product}-nprod"
        dependencies = [
            (value(task, "task_key"), [value(d, "task_key") for d in value(task, "depends_ons") or []])
            for task in tasks
        ]
        assert dependencies == [
            ("schema", []),
            ("transformation", ["schema"]),
            # After the load: a failed load leaves the tables unchanged, which the latency alert detects.
            ("metadata", ["transformation"]),
            ("quality", ["transformation"]),
            ("scd", ["quality"]),
            # Once the comments of the tables are set.
            ("genie", ["metadata"]),
            # Once the quality step has added its result columns.
            ("dashboard", ["quality"]),
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
            "data_product": data_product,
            "source_path": f"/Volumes/ext/{group}/{product}",
            "max_workers": "4",
            "synthetic_data": "false",
            "genie_warehouse_id": "warehouse-of-Serverless Starter Warehouse",
            "genie_parent_path": "/Workspace/Shared/cds-demo/nprod/genie",
            "dashboard_warehouse_id": "warehouse-of-Serverless Starter Warehouse",
            "dashboard_parent_path": "/Workspace/Shared/cds-demo/nprod/dashboards",
        }
        assert value(run_as, "service_principal_name") == "bf175dc4-c86b-4e88-83e9-9194c485a080"

    return pulumi.Output.all(
        *(pulumi.Output.all(job.name, job.tasks, job.environments, job.parameters, job.run_as) for job in jobs.values())
    ).apply(check)


def test_the_first_data_product_keeps_the_names_of_its_resources() -> None:
    """The resources of req were named before the demo had several data products: Pulumi renames them."""
    [alias] = (
        pulumi.ResourceOptions.merge(pulumi.ResourceOptions(), pipeline.renamed("donnees_qc.req", "pipeline")).aliases
        or []
    )
    assert isinstance(alias, pulumi.Alias) and alias.name == "pipeline"
    assert not pipeline.renamed("donnees_qc.repertoire_municipalites", "pipeline").aliases


@pulumi.runtime.test
def test_files(settings: pipeline.Settings) -> pulumi.Output[None]:
    jobs = pipeline.deploy(settings)

    def check(_: list[Any]) -> None:
        paths = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/workspaceFile:WorkspaceFile"}
        # The contracts, their partial documents and their configurations.
        sources = [
            path
            for pattern in ("*.py", "*.yaml")
            for path in pipeline.PRODUCTS.rglob(pattern)
            if "__pycache__" not in path.parts
        ]
        assert sources
        root = "/Shared/cds-demo/nprod/products"
        assert {f"{root}/{path.relative_to(pipeline.PRODUCTS).as_posix()}" for path in sources} <= paths
        assert not any("__pycache__" in path for path in paths)
        notebooks = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/notebook:Notebook"}
        assert notebooks == {f"/Shared/cds-demo/nprod/notebooks/{service}" for service in pipeline.TASKS}
        directories = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/directory:Directory"}
        assert {path.rsplit("/", 1)[0] for path in paths | notebooks} <= directories
        assert "/Shared/cds-demo/nprod" in directories

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_quality_dashboard(settings: pipeline.Settings) -> pulumi.Output[None]:
    """The job creates and publishes the dashboard, in the dashboards folder: the deployment does not."""
    CREATED.clear()
    jobs = pipeline.deploy(settings)

    def check(_: list[Any]) -> None:
        assert not [args for args in CREATED if args.typ == "databricks:index/dashboard:Dashboard"]
        folders = {
            args.inputs["path"]: args.inputs.get("deleteRecursive")
            for args in CREATED
            if args.typ == "databricks:index/directory:Directory"
        }
        # Deleting the folder deletes the dashboard the job created in it.
        assert folders["/Shared/cds-demo/nprod/dashboards"] is True

    return done(jobs).apply(check)


def created(typ: str, prefix: str = "") -> list[pulumi.runtime.MockResourceArgs]:
    return [args for args in CREATED if args.typ == typ and args.name.startswith(prefix)]


@pulumi.runtime.test
def test_synthetic_data(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(dataclasses.replace(settings, synthetic_data=True))

    def check(parameters: list[Sequence[Any] | None]) -> None:
        files = created("databricks:index/file:File")
        uploads = {args.inputs["path"]: args.inputs["source"] for args in files}
        # Named after their data product: two data products may publish files of the same name.
        assert sorted(args.name for args in files) == sorted(
            f"testdata/{data_product}/{Path(args.inputs['source']).name}"
            for args in files
            for data_product in PRODUCTS
            if Path(args.inputs["source"]).parent == pipeline.TESTDATA / data_product.replace(".", "/")
        )
        expected: dict[str, Path] = {}
        # The data products of the synthetic data set: the others load their published files.
        synthetic = [data_product for data_product in PRODUCTS if pipeline.has_testdata(settings, data_product)]
        assert "donnees_qc.req" in synthetic
        for data_product in synthetic:
            folder = pipeline.TESTDATA / data_product.replace(".", "/")
            names = sorted(path.name for path in folder.glob("*.csv"))
            assert names, data_product
            expected |= {f"/Volumes/ext/{data_product.replace('.', '/')}/{name}": folder / name for name in names}
        assert {path: Path(source) for path, source in uploads.items()} == expected
        for data_product, job_parameters in zip(PRODUCTS, parameters, strict=True):
            values = {value(parameter, "name"): value(parameter, "default") for parameter in job_parameters or []}
            assert values["synthetic_data"] == ("true" if data_product in synthetic else "false"), data_product

    return pulumi.Output.all(*(job.parameters for job in jobs.values())).apply(check)


@pulumi.runtime.test
def test_no_catalog_nor_synthetic_data_by_default(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(settings)

    def check(_: list[Any]) -> None:
        assert not created("databricks:index/catalog:Catalog")
        assert not created("databricks:index/grant:Grant")
        assert not created("databricks:index/file:File")

    return done(jobs).apply(check)


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
    jobs = pipeline.deploy(prod)

    def check(_: list[Any]) -> None:
        [catalog] = created("databricks:index/catalog:Catalog")
        assert catalog.inputs["name"] == "prod"
        assert catalog.inputs["storageRoot"] == "s3://bucket/prod"
        [grant] = created("databricks:index/grant:Grant")
        privileges = pipeline.READER_PRIVILEGES
        assert grant.inputs == {"catalog": "prod", "principal": "data-readers", "privileges": privileges}

    return done(jobs).apply(check)


def stack_config(stack: str) -> dict[str, str]:
    """Returns the values of the cds-demo namespace of a stack file (one per line, quotes removed)."""
    text = (pipeline.REPOSITORY / "cds-demo" / "deploy" / f"Pulumi.{stack}.yaml").read_text(encoding="utf-8")
    values = re.findall(r"^  cds-demo:(\w+): *(.+)$", text, flags=re.MULTILINE)
    return {key: raw.strip().strip('"') for key, raw in values}


def test_the_stacks_load_synthetic_data_in_nprod_only() -> None:
    nprod, prod = stack_config("nprod"), stack_config("prod")
    assert (nprod["syntheticData"], nprod["createCatalog"]) == ("true", "false")
    # The account uses Default Storage: the catalogs are created in Catalog Explorer.
    assert (prod["syntheticData"], prod["createCatalog"]) == ("false", "false")
    assert nprod["sourceRoot"] == prod["sourceRoot"]


def test_a_data_product_without_synthetic_data_set_loads_its_published_files(
    settings: pipeline.Settings, tmp_path: Path
) -> None:
    testdata = dataclasses.replace(settings, synthetic_data=True, testdata_directory=tmp_path)
    (tmp_path / "donnees_qc" / "req").mkdir(parents=True)

    assert pipeline.has_testdata(testdata, "donnees_qc.req")
    assert not pipeline.has_testdata(testdata, "donnees_qc.other")


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
    # Folders receiving objects that are not files, such as the alerts.
    assert pipeline.folders("/r", ["/r/a/file"], ["/r/d/e", "/elsewhere"]) == ["/r", "/r/a", "/r/d", "/r/d/e"]


def test_missing_wheel(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="uv build --package cds-framework"):
        pipeline.framework_wheel(tmp_path)


DYNAMIC = "pulumi-python:dynamic:Resource"


@pulumi.runtime.test
def test_masking(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    masked_settings = dataclasses.replace(
        settings, masking=True, create_tag_policies=True, masking_exempt_principals=("data-stewards",)
    )
    jobs = pipeline.deploy(masked_settings)

    def check(_: list[Any]) -> None:
        [tag_policy] = created("databricks:index/tagPolicy:TagPolicy")
        assert tag_policy.inputs["tagKey"] == CLASSIFICATION_TAG
        # The policies wait for the new governed tag to propagate.
        [delay] = created(DYNAMIC, f"tag-policy-propagation:{CLASSIFICATION_TAG}")
        assert delay.inputs["seconds"] == pipeline.TAG_POLICY_PROPAGATION_SECONDS
        assert delay.inputs["trigger"] == f"tag-policy:{CLASSIFICATION_TAG}-id"
        names = [value(item, "name") for item in tag_policy.inputs["values"]]
        assert names == MANIFEST["classification"]["values"]
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
        [masked] = masked_settings.masked_classifications
        assert inputs["name"] == f"mask_{masked}"
        assert (inputs["onSecurableType"], inputs["onSecurableFullname"]) == ("CATALOG", "nprod")
        assert (inputs["forSecurableType"], inputs["policyType"]) == ("TABLE", "POLICY_TYPE_COLUMN_MASK")
        assert inputs["toPrincipals"] == ["account users"]
        # The service principal running the job reads the data as it is.
        assert inputs["exceptPrincipals"] == ["bf175dc4-c86b-4e88-83e9-9194c485a080", "data-stewards"]
        [match] = inputs["matchColumns"]
        assert value(match, "condition") == f"has_tag_value('{CLASSIFICATION_TAG}', '{masked}')"
        mask = inputs["columnMask"]
        assert value(mask, "function_name") == "nprod.governance.mask"
        assert value(mask, "on_column") == value(match, "alias")

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_masking_without_creating_the_tag_policy(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(dataclasses.replace(settings, masking=True))

    def check(_: list[Any]) -> None:
        assert not created("databricks:index/tagPolicy:TagPolicy")
        assert not created(DYNAMIC, "tag-policy-propagation")
        assert len(created("databricks:index/policyInfo:PolicyInfo")) == 1

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_no_masking_by_default(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(settings)

    def check(_: list[Any]) -> None:
        assert not created("databricks:index/policyInfo:PolicyInfo")
        assert not created("databricks:index/schema:Schema")
        assert not created(DYNAMIC)

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_masking_nothing(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(dataclasses.replace(settings, masking=True, masked_classifications=()))

    def check(_: list[Any]) -> None:
        assert not created("databricks:index/policyInfo:PolicyInfo")
        assert created(DYNAMIC, "mask-function")

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_the_tag_policy_is_protected(settings: pipeline.Settings) -> None:
    """Destroying the stack creating the tag policy must not remove the masking of the other stacks."""
    manifest = pipeline.load_manifest(settings.manifest)
    [tag_policy, _] = pipeline.governance(
        dataclasses.replace(settings, masking=True, create_tag_policies=True), manifest, "w", None
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
        pipeline.governance(dataclasses.replace(settings, masking=True, service_principal=None), {}, "w", None)
    with pytest.raises(ValueError, match="Unknown masked classification"):
        manifest = pipeline.load_manifest(settings.manifest)
        pipeline.governance(dataclasses.replace(settings, masked_classifications=("secret",)), manifest, "w", None)
    with pytest.raises(FileNotFoundError, match=r"manifest\.py"):
        pipeline.load_manifest(tmp_path / "manifest.json")
    (tmp_path / "manifest.json").write_text(json.dumps({**MANIFEST, "data_products": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="lists no data product"):
        pipeline.load_manifest(tmp_path / "manifest.json")


def test_the_stacks_mask_the_classified_columns() -> None:
    nprod, prod = stack_config("nprod"), stack_config("prod")
    assert nprod["masking"] == prod["masking"] == "true"
    # The tag policy belongs to the account: the nprod stack creates it, protected, for both.
    assert (nprod["createTagPolicies"], prod["createTagPolicies"]) == ("true", "false")


@pulumi.runtime.test
def test_discover_domains(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(dataclasses.replace(settings, create_domains=True))

    def check(_: list[Any]) -> None:
        # The governed tags of the domains of the manifest, without values.
        policies = {args.inputs["tagKey"]: args.inputs for args in created("databricks:index/tagPolicy:TagPolicy")}
        assert sorted(policies) == sorted(DOMAIN_TAGS)
        assert all("values" not in inputs for inputs in policies.values())
        [delay] = created(DYNAMIC, "tag-policy-propagation:domains")
        assert delay.inputs["seconds"] == pipeline.TAG_POLICY_PROPAGATION_SECONDS
        assert delay.inputs["trigger"] == ",".join(f"tag-policy:{tag}-id" for tag in DOMAIN_TAGS)
        domains = {args.inputs["tagKey"]: args.inputs for args in created("databricks:index/domain:Domain")}
        assert sorted(domains) == sorted(DOMAIN_TAGS)
        # A subdomain is created under its parent; all are published.
        for domain in MANIFEST["domains"]:
            inputs = domains[domain["tag"]]
            if domain["parent"] is None:
                assert "parentDomainId" not in inputs
            else:
                assert inputs["parentDomainId"] == f"domain:{domain['parent']}-domain-id"
            assert inputs["subtitle"] == pipeline.DOMAINS[domain["tag"]].subtitle
        assert all(inputs["draft"] is False for inputs in domains.values())
        assert all(len(inputs["subtitle"]) <= 280 for inputs in domains.values())

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_no_discover_domains_by_default(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(settings)

    def check(_: list[Any]) -> None:
        assert not created("databricks:index/domain:Domain")
        assert not created("databricks:index/tagPolicy:TagPolicy")

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_the_discover_domains_are_protected(settings: pipeline.Settings) -> None:
    """Destroying the stack creating the domains must not remove those of the other stacks."""
    CREATED.clear()
    resources = pipeline.discover_domains(pipeline.load_manifest(settings.manifest))

    # The job waits for the governed tags it applies, after their propagation.
    names = [type(resource).__name__ for resource in resources]
    count = len(MANIFEST["domains"])
    assert names == ["TagPolicy"] * count + ["Delay"] + ["Domain"] * count
    account = [resource for resource in resources if not isinstance(resource, Delay)]
    assert all(isinstance(resource, pulumi.CustomResource) and resource._protect for resource in account)


def test_discover_domain_errors() -> None:
    CREATED.clear()
    with pytest.raises(ValueError, match="Describe the Discover domain"):
        pipeline.discover_domains({"domains": [{"tag": "Santé", "parent": None}]})
    with pytest.raises(ValueError, match="needs its parent 'Santé' before it"):
        pipeline.discover_domains(
            {"domains": [{"tag": "Santé/Hôpitaux", "parent": "Santé"}, {"tag": "Santé", "parent": None}]}
        )
    assert pipeline.discover_domains({}) == []
    # Nothing is created when the manifest is invalid.
    assert not CREATED


def test_the_manifest_domains_are_described() -> None:
    assert set(DOMAIN_TAGS) <= set(pipeline.DOMAINS)


def test_the_stacks_create_the_discover_domains_in_nprod_only() -> None:
    # The domains belong to the account: the nprod stack creates them, protected, for both.
    assert (stack_config("nprod")["createDomains"], stack_config("prod")["createDomains"]) == ("true", "false")


@pulumi.runtime.test
def test_the_job_runs_on_the_frequency_of_the_contracts(settings: pipeline.Settings) -> pulumi.Output[None]:
    paused = pipeline.deploy(dataclasses.replace(settings, pause_schedule=True))
    running = pipeline.deploy(dataclasses.replace(settings, pause_schedule=False, timezone="UTC"))

    def check(schedules: list[Any]) -> None:
        for data_product, paused_schedule, running_schedule in zip(
            PRODUCTS, schedules[: len(PRODUCTS)], schedules[len(PRODUCTS) :], strict=True
        ):
            # The frequency of the contracts of the data product.
            assert value(paused_schedule, "quartz_cron_expression") == PRODUCTS[data_product]["schedule"]["quartz"]
            assert (value(paused_schedule, "timezone_id"), value(paused_schedule, "pause_status")) == (
                "America/Toronto",
                "PAUSED",
            )
            assert (value(running_schedule, "timezone_id"), value(running_schedule, "pause_status")) == (
                "UTC",
                "UNPAUSED",
            )

    return pulumi.Output.all(
        *(paused[data_product].schedule for data_product in PRODUCTS),
        *(running[data_product].schedule for data_product in PRODUCTS),
    ).apply(check)


@pulumi.runtime.test
def test_alerts(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(dataclasses.replace(settings, alert_emails=("data-team@example.org",)))

    def check(_: list[Any]) -> None:
        destinations = {
            args.name: args.inputs
            for args in created("databricks:index/notificationDestination:NotificationDestination")
        }
        assert sorted(destinations) == sorted(f"alert-destination:{data_product}" for data_product in PRODUCTS)
        for data_product, product in PRODUCTS.items():
            destination = destinations[f"alert-destination:{data_product}"]
            assert destination["displayName"] == f"CDS demo alerts, {data_product} (nprod)"
            email = value(destination["config"], "email")
            # The email support channel of the contracts, and those of the stack.
            assert value(email, "addresses") == [*product["recipients"], "data-team@example.org"]

        alerts = {args.name: args.inputs for args in created("databricks:index/alertV2:AlertV2")}
        assert sorted(alerts) == sorted(
            f"alert:{data_product}:{alert['name']}"
            for data_product, product in PRODUCTS.items()
            for alert in product["alerts"]
        )
        for name, inputs in alerts.items():
            # As the API returns it: the provider sends it again with every update.
            assert inputs["parentPath"] == "/Workspace/Shared/cds-demo/nprod/alerts"
            assert inputs["warehouseId"] == "warehouse-of-Serverless Starter Warehouse"
            assert "${environment}" not in inputs["queryText"], name
            assert value(inputs["runAs"], "service_principal_name") == "bf175dc4-c86b-4e88-83e9-9194c485a080"
            evaluation = inputs["evaluation"]
            source = value(evaluation, "source")
            assert (value(source, "name"), value(source, "aggregation")) == ("exceeded", "MAX")
            assert value(evaluation, "comparison_operator") == "GREATER_THAN"
            assert value(value(value(evaluation, "threshold"), "value"), "double_value") == 0
            [subscription] = value(value(evaluation, "notification"), "subscriptions")
            data_product = name.split(":")[1]
            assert value(subscription, "destination_id") == f"alert-destination:{data_product}-id"
            schedule = inputs["schedule"]
            assert (value(schedule, "timezone_id"), value(schedule, "pause_status")) == ("America/Toronto", "UNPAUSED")
        # The schedules and the queries of the manifest, in the environment.
        for data_product, product in PRODUCTS.items():
            for alert in product["alerts"]:
                inputs = alerts[f"alert:{data_product}:{alert['name']}"]
                assert value(inputs["schedule"], "quartz_cron_schedule") == alert["quartz"]
                assert inputs["queryText"] == pipeline.environment_query(alert["query"], "nprod")
                assert inputs["displayName"] == f"{data_product} {alert['title']} SLA (nprod)"
        # The failed runs of each job email the same addresses.
        jobs_args = {args.name: args for args in created("databricks:index/job:Job")}
        for data_product, product in PRODUCTS.items():
            failures = value(jobs_args[f"pipeline:{data_product}"].inputs["emailNotifications"], "on_failures")
            assert failures == [*product["recipients"], "data-team@example.org"]
        directories = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/directory:Directory"}
        assert "/Shared/cds-demo/nprod/alerts" in directories

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_alerts_without_recipients(settings: pipeline.Settings, tmp_path: Path) -> pulumi.Output[None]:
    CREATED.clear()
    manifest = with_products(lambda product: {**product, "recipients": []})
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    jobs = pipeline.deploy(dataclasses.replace(settings, manifest=tmp_path / "manifest.json"))

    def check(_: list[Any]) -> None:
        assert not created("databricks:index/notificationDestination:NotificationDestination")
        alerts = created("databricks:index/alertV2:AlertV2")
        assert len(alerts) == sum(len(product["alerts"]) for product in PRODUCTS.values())
        assert all(value(args.inputs["evaluation"], "notification") is None for args in alerts)
        jobs_args = created("databricks:index/job:Job")
        assert len(jobs_args) == len(PRODUCTS)
        assert all("emailNotifications" not in args.inputs for args in jobs_args)

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_no_schedule_nor_alerts_without_sla(settings: pipeline.Settings, tmp_path: Path) -> pulumi.Output[None]:
    CREATED.clear()
    manifest = with_products(lambda product: {**product, "schedule": None, "alerts": []})
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    jobs = pipeline.deploy(dataclasses.replace(settings, manifest=tmp_path / "manifest.json"))

    def check(schedules: list[Any]) -> None:
        assert schedules == [None] * len(PRODUCTS)
        # The failed runs of each job still email the support channel.
        jobs_args = {args.name: args for args in created("databricks:index/job:Job")}
        for data_product, product in PRODUCTS.items():
            notifications = jobs_args[f"pipeline:{data_product}"].inputs["emailNotifications"]
            assert value(notifications, "on_failures") == product["recipients"]
        assert not created("databricks:index/alertV2:AlertV2")
        assert not created("databricks:index/notificationDestination:NotificationDestination")
        directories = {args.inputs["path"] for args in CREATED if args.typ == "databricks:index/directory:Directory"}
        assert "/Shared/cds-demo/nprod/alerts" not in directories

    return pulumi.Output.all(*(job.schedule for job in jobs.values())).apply(check)


def test_environment_query() -> None:
    assert pipeline.environment_query("SELECT * FROM `${environment}`.`s`.`t`", "nprod") == (
        "SELECT * FROM `nprod`.`s`.`t`"
    )
    with pytest.raises(ValueError, match="Invalid environment"):
        pipeline.environment_query("SELECT 1", "n'prod")


def test_the_stacks_pause_the_schedule_in_nprod_only() -> None:
    nprod, prod = stack_config("nprod"), stack_config("prod")
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


@pulumi.runtime.test
def test_genie_folder(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(dataclasses.replace(settings, genie_users=("users", "someone@example.com")))

    def check(_: list[Any]) -> None:
        folders = {
            args.inputs["path"]: args.inputs.get("deleteRecursive")
            for args in CREATED
            if args.typ == "databricks:index/directory:Directory"
        }
        # The space in the folder is created by the job: deleting the folder deletes it.
        assert folders["/Shared/cds-demo/nprod/genie"] is True
        assert not folders["/Shared/cds-demo/nprod/notebooks"]
        [permissions] = [args for args in CREATED if args.typ == "databricks:index/permissions:Permissions"]
        assert permissions.inputs["directoryPath"] == "/Shared/cds-demo/nprod/genie"
        assert permissions.inputs["accessControls"] == [
            {"groupName": "users", "permissionLevel": "CAN_RUN"},
            {"userName": "someone@example.com", "permissionLevel": "CAN_RUN"},
        ]

    return done(jobs).apply(check)


@pulumi.runtime.test
def test_no_genie_users_by_default(settings: pipeline.Settings) -> pulumi.Output[None]:
    CREATED.clear()
    jobs = pipeline.deploy(settings)

    def check(_: list[Any]) -> None:
        assert not created("databricks:index/permissions:Permissions")

    return done(jobs).apply(check)


def test_access_control() -> None:
    sp = "bf175dc4-c86b-4e88-83e9-9194c485a080"

    assert pipeline.access_control("users", "CAN_RUN").group_name == "users"
    assert pipeline.access_control("someone@example.com", "CAN_RUN").user_name == "someone@example.com"
    assert pipeline.access_control(sp, "CAN_RUN").service_principal_name == sp


def test_the_stacks_grant_the_genie_space_to_the_users() -> None:
    for stack in ("nprod", "prod"):
        assert json.loads(stack_config(stack)["genieUsers"]) == ["users"], stack
