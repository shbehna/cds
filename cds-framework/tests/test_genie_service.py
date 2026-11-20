"""Tests of the Genie service, with a fake Genie API and a fake Spark session."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from conftest import PROJECT, MakeProject
from databricks.sdk.errors import NotFound, PermissionDenied
from fakes import FakeSpark, FakeTagAssignments
from test_genie import GENIE_CONFIG

from cds import DataContext, GenieService, RuntimeConfiguration, ServiceContext, Status
from cds.genie import build_space
from cds.services.genie import DOMAINS_PROPERTY, SPACE_PROPERTY

SCHEMA_PATH = "/api/2.1/unity-catalog/schemas/dev.sales_orders"


class FakeApi:
    """The Genie spaces and the schema properties of a workspace, as its REST API serves them."""

    def __init__(self, properties: Mapping[str, str] | None = None) -> None:
        self.properties: dict[str, str] = dict(properties or {})
        self.spaces: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str]] = []
        self.tag_api = FakeTagAssignments()

    def do(
        self,
        method: str,
        path: str,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls.append((method, path))
        if self.tag_api.handles(path):
            return self.tag_api.do(method, path, body)
        if path == SCHEMA_PATH and method == "GET":
            return {"full_name": "dev.sales_orders", "properties": dict(self.properties)}
        if path == "/api/2.0/genie/spaces" and method == "POST":
            space_id = f"{len(self.spaces) + 1:032x}"
            # The API returns the parent folder without its /Workspace prefix.
            parent = str((body or {}).get("parent_path", "/Users/sp")).removeprefix("/Workspace")
            self.spaces[space_id] = {**(body or {}), "space_id": space_id, "parent_path": parent}
            return {key: value for key, value in self.spaces[space_id].items() if key != "serialized_space"}
        space_id = path.rsplit("/", 1)[1]
        if space_id not in self.spaces:
            raise NotFound(f"Space {space_id} not found")
        if method == "GET":
            space = dict(self.spaces[space_id])
            if not (query or {}).get("include_serialized_space"):
                space.pop("serialized_space")
            return space
        if method == "PATCH":
            self.spaces[space_id].update(body or {})
            return self.spaces[space_id]
        if method == "DELETE":
            # Deleted, the space goes to the trash: the API then answers NOT_FOUND.
            del self.spaces[space_id]
            return {}
        raise AssertionError(f"unexpected call {method} {path}")


def service_for(
    root: Path, spark: FakeSpark, api: FakeApi, parameters: Mapping[str, str] | None = None
) -> GenieService:
    client = MagicMock()
    client.api_client = api
    runtime = RuntimeConfiguration({"environment": "dev", "genie_warehouse_id": "abc123", **(parameters or {})})
    context = ServiceContext(
        spark=spark.as_session(),
        environment="dev",
        data_context=DataContext.discover(root),
        runtime_configuration=runtime,
        workspace_client=client,
    )
    return GenieService(context)


@pytest.fixture
def root(make_project: MakeProject) -> Path:
    return make_project({**PROJECT, "sales/orders/orders_config.py": GENIE_CONFIG})


def run(service: GenieService) -> tuple[Status, str]:
    data_product = service.context.data_context.get_data_product("sales.orders")
    [result] = service.run(data_product).results
    return result.status, result.message


def test_the_space_is_created_and_recorded_in_the_schema(root: Path, spark: FakeSpark) -> None:
    api = FakeApi()
    service = service_for(root, spark, api, {"genie_parent_path": "/Workspace/Shared/genie"})

    status, message = run(service)

    assert status is Status.SUCCESS
    [(space_id, space)] = api.spaces.items()
    assert message == (f"created the Genie space {space_id} (sales.orders (dev)): 2 table(s), 1 join(s), 3 example(s)")
    assert space["title"] == "sales.orders (dev)"
    assert space["description"] == "Orders of the shop."
    assert space["warehouse_id"] == "abc123"
    data_product = service.context.data_context.get_data_product("sales.orders")
    assert json.loads(space["serialized_space"]) == build_space(data_product, service.context)["serialized_space"]
    assert spark.statements == [
        f"ALTER SCHEMA `dev`.`sales_orders` SET DBPROPERTIES ('{SPACE_PROPERTY}' = '{space_id}')"
    ]


def test_an_unchanged_space_is_not_updated(root: Path, spark: FakeSpark) -> None:
    api = FakeApi()
    run(service_for(root, spark, api, {"genie_parent_path": "/Workspace/Shared/genie"}))
    [space_id] = api.spaces
    api.properties[SPACE_PROPERTY] = space_id
    api.calls.clear()

    status, message = run(service_for(root, spark, api, {"genie_parent_path": "/Workspace/Shared/genie"}))

    assert (status, message.split(" (")[0]) == (Status.SUCCESS, f"unchanged the Genie space {space_id}")
    assert [method for method, _ in api.calls] == ["GET", "GET"]


def test_a_changed_space_is_updated(root: Path, spark: FakeSpark, make_project: MakeProject) -> None:
    api = FakeApi()
    run(service_for(root, spark, api))
    [space_id] = api.spaces
    api.properties[SPACE_PROPERTY] = space_id
    spark.statements.clear()
    changed = make_project(
        {
            **PROJECT,
            "sales/orders/orders_config.py": GENIE_CONFIG.replace("Amounts are in euros.", "Amounts are in dollars."),
        },
        name="changed",
    )

    status, message = run(service_for(changed, spark, api, {"genie_warehouse_id": "def456"}))

    assert status is Status.SUCCESS
    assert message.startswith(f"updated the Genie space {space_id} ")
    assert ("PATCH", f"/api/2.0/genie/spaces/{space_id}") in api.calls
    assert "Amounts are in dollars." in api.spaces[space_id]["serialized_space"]
    assert api.spaces[space_id]["warehouse_id"] == "def456"
    assert spark.statements == [], "the id of the space does not change"


def test_a_deleted_space_is_created_again(root: Path, spark: FakeSpark) -> None:
    api = FakeApi({SPACE_PROPERTY: "0" * 32})

    status, message = run(service_for(root, spark, api))

    assert status is Status.SUCCESS
    [space_id] = api.spaces
    assert message.startswith(f"created the Genie space {space_id} ")
    assert spark.statements_matching(f"SET DBPROPERTIES \\('{SPACE_PROPERTY}' = '{space_id}'\\)")


def test_the_service_is_skipped_without_a_genie_space(project: Path, spark: FakeSpark) -> None:
    api = FakeApi()

    assert run(service_for(project, spark, api)) == (
        Status.SKIPPED,
        "no Genie space: GenieSettings.enabled is not set",
    )
    assert api.calls == []


def test_the_warehouse_is_required(root: Path, spark: FakeSpark) -> None:
    api = FakeApi()
    service = service_for(root, spark, api, {"genie_warehouse_id": ""})

    status, message = run(service)

    assert status is Status.ERROR
    assert "genie_warehouse_id" in message
    assert api.spaces == {}


def test_a_failure_of_the_api_is_reported(root: Path, spark: FakeSpark) -> None:
    api = FakeApi()
    api.do = MagicMock(side_effect=RuntimeError("permission denied"))  # type: ignore[method-assign]

    report = service_for(root, spark, api).run(DataContext.discover(root).get_data_product("sales.orders"))

    assert report.has_errors
    assert report.results[0].message == "RuntimeError: permission denied"
    assert report.results[0].table == "sales.orders"


def test_the_service_does_not_process_tables(root: Path, spark: FakeSpark) -> None:
    service = service_for(root, spark, FakeApi())
    table = service.context.data_context.get_table("order")

    with pytest.raises(Exception, match="processes data products, not tables"):
        service.process(table)


def test_a_space_whose_id_cannot_be_recorded_is_deleted(root: Path, spark: FakeSpark) -> None:
    """Otherwise the next run would not find it, and would create another one."""
    api = FakeApi()
    spark.on(r"^ALTER SCHEMA", RuntimeError("permission denied"))

    status, message = run(service_for(root, spark, api))

    assert (status, message) == (Status.ERROR, "RuntimeError: permission denied")
    assert api.spaces == {}
    assert [method for method, _ in api.calls] == ["GET", "POST", "DELETE"]


def test_a_removed_description_is_cleared(root: Path, spark: FakeSpark, make_project: MakeProject) -> None:
    api = FakeApi()
    run(service_for(root, spark, api))
    [space_id] = api.spaces
    api.properties[SPACE_PROPERTY] = space_id
    without = make_project(
        {**PROJECT, "sales/orders/orders_config.py": GENIE_CONFIG.replace('description="Orders of the shop.",', "")},
        name="without",
    )

    assert run(service_for(without, spark, api))[1].startswith("updated ")
    assert api.spaces[space_id]["description"] == ""
    # Cleared, the description no longer differs.
    assert run(service_for(without, spark, api))[1].startswith("unchanged ")


def test_the_space_joins_the_discover_domain_of_its_tables(make_project: MakeProject, spark: FakeSpark) -> None:
    group = """
        from cds import GroupConfig, MetadataSettings

        config = GroupConfig(
            name="sales",
            contract_defaults={"domain": "Ventes"},
            metadata=MetadataSettings(discover_domain=True),
        )
    """
    root = make_project({**PROJECT, "sales/sales_config.py": group, "sales/orders/orders_config.py": GENIE_CONFIG})
    api = FakeApi()

    status, message = run(service_for(root, spark, api))

    [space_id] = api.spaces
    assert (status, message.rsplit(", ", 1)[1]) == (Status.SUCCESS, "in the Discover domain(s) Ventes")
    assert api.tag_api.of("geniespaces", space_id) == {"Ventes"}
    assert spark.statements_matching(rf"'{DOMAINS_PROPERTY}' = '\[\"Ventes\"\]'")

    api.tag_api.error = PermissionDenied("User does not have ASSIGN on the tag Ventes")
    api.properties.update({SPACE_PROPERTY: space_id, DOMAINS_PROPERTY: "[]"})
    status, message = run(service_for(root, spark, api))

    assert status is Status.WARNING
    assert message.startswith(f"unchanged the Genie space {space_id}")
    assert message.endswith(
        "; cannot tag it with its Discover domain(s): PermissionDenied: User does not have ASSIGN on the tag Ventes"
    )
