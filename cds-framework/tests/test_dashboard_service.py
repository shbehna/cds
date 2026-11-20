"""Tests of the dashboard service, with a fake Lakeview API and a fake Spark session."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from conftest import PROJECT, MakeProject
from databricks.sdk.errors import (
    AlreadyExists,
    NotFound,
    PermissionDenied,
    ResourceAlreadyExists,
)
from fakes import TAG_ASSIGNMENTS, FakeSpark, FakeTagAssignments
from test_dashboard import DASHBOARD_CONFIG

from cds import DashboardService, DataContext, RuntimeConfiguration, ServiceContext, Status
from cds.dashboard import build_dashboard
from cds.services.dashboard import DASHBOARD_PROPERTY, DOMAINS_PROPERTY

SCHEMA_PATH = "/api/2.1/unity-catalog/schemas/dev.sales_orders"
DASHBOARDS = "/api/2.0/lakeview/dashboards"


class FakeLakeview:
    """The dashboards and the schema properties of a workspace, as its REST API serves them."""

    def __init__(self, properties: Mapping[str, str] | None = None) -> None:
        self.properties: dict[str, str] = dict(properties or {})
        self.dashboards: dict[str, dict[str, Any]] = {}
        self.published: dict[str, dict[str, Any]] = {}
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
        if path == DASHBOARDS and method == "POST":
            dashboard_id = f"{len(self.dashboards) + 1:032x}"
            parent = str((body or {}).get("parent_path", "/Users/sp")).removeprefix("/Workspace")
            self.dashboards[dashboard_id] = {
                **(body or {}),
                "dashboard_id": dashboard_id,
                "parent_path": parent,
                "lifecycle_state": "ACTIVE",
            }
            return self.dashboards[dashboard_id]
        parts = path.removeprefix(f"{DASHBOARDS}/").split("/")
        dashboard_id = parts[0]
        if dashboard_id not in self.dashboards:
            raise NotFound(f"Dashboard {dashboard_id} not found")
        if parts[1:] == ["published"]:
            if method == "POST":
                self.published[dashboard_id] = dict(body or {})
                return self.published[dashboard_id]
            if dashboard_id not in self.published:
                raise NotFound(f"Dashboard {dashboard_id} is not published")
            return self.published[dashboard_id]
        if method == "GET":
            return self.dashboards[dashboard_id]
        if method == "PATCH":
            self.dashboards[dashboard_id].update(body or {})
            return self.dashboards[dashboard_id]
        if method == "DELETE":
            # Deleted, a dashboard goes to the trash, where it can still be read.
            self.dashboards[dashboard_id]["lifecycle_state"] = "TRASHED"
            return {}
        raise AssertionError(f"unexpected call {method} {path}")


def service_for(
    root: Path, spark: FakeSpark, api: FakeLakeview, parameters: Mapping[str, str] | None = None
) -> DashboardService:
    client = MagicMock()
    client.api_client = api
    runtime = RuntimeConfiguration(
        {
            "environment": "dev",
            "dashboard_warehouse_id": "abc123",
            "dashboard_parent_path": "/Workspace/Shared/dashboards",
            **(parameters or {}),
        }
    )
    context = ServiceContext(
        spark=spark.as_session(),
        environment="dev",
        data_context=DataContext.discover(root),
        runtime_configuration=runtime,
        workspace_client=client,
    )
    return DashboardService(context)


@pytest.fixture
def root(make_project: MakeProject) -> Path:
    return make_project({**PROJECT, "sales/orders/orders_config.py": DASHBOARD_CONFIG})


def run(service: DashboardService) -> tuple[Status, str]:
    [result] = service.run(service.context.data_context.get_data_product("sales.orders")).results
    return result.status, result.message


def created_once(root: Path, spark: FakeSpark, api: FakeLakeview) -> str:
    run(service_for(root, spark, api))
    [dashboard_id] = api.dashboards
    api.properties[DASHBOARD_PROPERTY] = dashboard_id
    api.calls.clear()
    spark.statements.clear()
    return dashboard_id


def test_the_dashboard_is_created_published_and_recorded(root: Path, spark: FakeSpark) -> None:
    api = FakeLakeview()
    service = service_for(root, spark, api)

    status, message = run(service)

    [(dashboard_id, dashboard)] = api.dashboards.items()
    assert (status, message) == (
        Status.SUCCESS,
        f"created the dashboard {dashboard_id} (sales.orders data quality (dev)), published",
    )
    assert dashboard["parent_path"] == "/Shared/dashboards"
    assert dashboard["warehouse_id"] == "abc123"
    data_product = service.context.data_context.get_data_product("sales.orders")
    expected = build_dashboard(data_product, service.context)["serialized_dashboard"]
    assert json.loads(dashboard["serialized_dashboard"]) == expected
    # Its viewers see the data with the credentials of the service.
    assert api.published[dashboard_id] == {"embed_credentials": True, "warehouse_id": "abc123"}
    assert spark.statements == [
        f"ALTER SCHEMA `dev`.`sales_orders` SET DBPROPERTIES ('{DASHBOARD_PROPERTY}' = '{dashboard_id}')"
    ]


def test_an_unchanged_published_dashboard_is_left_alone(root: Path, spark: FakeSpark) -> None:
    api = FakeLakeview()
    dashboard_id = created_once(root, spark, api)

    status, message = run(service_for(root, spark, api))

    assert (status, message) == (
        Status.SUCCESS,
        f"unchanged the dashboard {dashboard_id} (sales.orders data quality (dev)), already published",
    )
    assert [method for method, _ in api.calls] == ["GET", "GET", "GET"]


def test_an_unpublished_dashboard_is_published(root: Path, spark: FakeSpark) -> None:
    api = FakeLakeview()
    dashboard_id = created_once(root, spark, api)
    del api.published[dashboard_id]

    assert run(service_for(root, spark, api))[1].endswith("), published")
    assert dashboard_id in api.published


def test_a_changed_dashboard_is_updated_and_published(root: Path, spark: FakeSpark, make_project: MakeProject) -> None:
    api = FakeLakeview()
    dashboard_id = created_once(root, spark, api)
    renamed = make_project(
        {
            **PROJECT,
            "sales/orders/orders_config.py": DASHBOARD_CONFIG.replace("enabled=True", 'enabled=True, title="Q"'),
        },
        name="renamed",
    )

    status, message = run(service_for(renamed, spark, api, {"dashboard_warehouse_id": "def456"}))

    assert (status, message) == (Status.SUCCESS, f"updated the dashboard {dashboard_id} (Q (dev)), published")
    assert api.dashboards[dashboard_id]["display_name"] == "Q (dev)"
    assert api.published[dashboard_id]["warehouse_id"] == "def456"
    assert spark.statements == [], "the id of the dashboard does not change"


def test_a_trashed_dashboard_is_created_again(root: Path, spark: FakeSpark) -> None:
    api = FakeLakeview()
    old = created_once(root, spark, api)
    api.do("DELETE", f"{DASHBOARDS}/{old}")

    status, message = run(service_for(root, spark, api))

    [new] = [key for key, value in api.dashboards.items() if value["lifecycle_state"] == "ACTIVE"]
    assert new != old
    assert (status, message.split(" (")[0]) == (Status.SUCCESS, f"created the dashboard {new}")
    assert spark.statements_matching(f"'{DASHBOARD_PROPERTY}' = '{new}'")


def test_a_dashboard_in_another_folder_is_reported(root: Path, spark: FakeSpark) -> None:
    api = FakeLakeview()
    created_once(root, spark, api)

    status, message = run(service_for(root, spark, api, {"dashboard_parent_path": "/Workspace/Shared/other"}))

    assert status is Status.WARNING
    assert message.endswith("it is in /Shared/dashboards, not in /Workspace/Shared/other: move it there")


def test_a_dashboard_whose_id_cannot_be_recorded_is_deleted(root: Path, spark: FakeSpark) -> None:
    api = FakeLakeview()
    spark.on(r"^ALTER SCHEMA", RuntimeError("permission denied"))

    assert run(service_for(root, spark, api)) == (Status.ERROR, "RuntimeError: permission denied")
    [dashboard] = api.dashboards.values()
    assert dashboard["lifecycle_state"] == "TRASHED"
    assert api.published == {}


def test_the_service_is_skipped_without_a_dashboard(project: Path, spark: FakeSpark) -> None:
    api = FakeLakeview()

    assert run(service_for(project, spark, api)) == (
        Status.SKIPPED,
        "no dashboard: DashboardSettings.enabled is not set",
    )
    assert api.calls == []


def test_the_warehouse_is_required(root: Path, spark: FakeSpark) -> None:
    status, message = run(service_for(root, spark, FakeLakeview(), {"dashboard_warehouse_id": ""}))

    assert status is Status.ERROR
    assert "dashboard_warehouse_id" in message


DOMAIN_GROUP = """
    from cds import GroupConfig, MetadataSettings

    config = GroupConfig(
        name="sales",
        contract_defaults={"domain": "Ventes"},
        metadata=MetadataSettings(discover_domain=True),
    )
"""


def with_domain(make_project: MakeProject, domain: str = "Ventes", name: str = "domain") -> Path:
    return make_project(
        {
            **PROJECT,
            "sales/sales_config.py": DOMAIN_GROUP.replace('"Ventes"', f'"{domain}"'),
            "sales/orders/orders_config.py": DASHBOARD_CONFIG,
        },
        name=name,
    )


def test_the_dashboard_joins_the_discover_domain_of_its_tables(make_project: MakeProject, spark: FakeSpark) -> None:
    api = FakeLakeview()

    status, message = run(service_for(with_domain(make_project), spark, api))

    [dashboard_id] = api.dashboards
    assert status is Status.SUCCESS
    assert message.endswith("), published, in the Discover domain(s) Ventes")
    assert api.tag_api.of("dashboards", dashboard_id) == {"Ventes"}
    assert spark.statements_matching(rf"'{DOMAINS_PROPERTY}' = '\[\"Ventes\"\]'")


def test_a_tagged_dashboard_is_not_tagged_again(make_project: MakeProject, spark: FakeSpark) -> None:
    api = FakeLakeview()
    root = with_domain(make_project)
    created_once(root, spark, api)
    api.properties[DOMAINS_PROPERTY] = '["Ventes"]'

    assert run(service_for(root, spark, api))[0] is Status.SUCCESS
    assert ("POST", TAG_ASSIGNMENTS) not in api.calls
    assert spark.statements == []


def test_a_changed_domain_replaces_the_tag_it_applied(make_project: MakeProject, spark: FakeSpark) -> None:
    api = FakeLakeview()
    dashboard_id = created_once(with_domain(make_project, "Ventes/Détail"), spark, api)
    api.properties[DOMAINS_PROPERTY] = '["Ventes/Détail"]'
    # Added in the Discover page: not a tag of the service.
    api.tag_api.tags[("dashboards", dashboard_id)].add("Autre/Domaine")

    status, message = run(service_for(with_domain(make_project, "Achats", name="achats"), spark, api))

    assert status is Status.SUCCESS
    assert message.endswith("in the Discover domain(s) Achats")
    assert api.tag_api.of("dashboards", dashboard_id) == {"Achats", "Autre/Domaine"}
    # The key of a subdomain tag is a single segment of the path: its slash is encoded.
    assert ("DELETE", f"{TAG_ASSIGNMENTS}/dashboards/{dashboard_id}/tags/Ventes%2FD%C3%A9tail") in api.calls
    assert spark.statements_matching(rf"'{DOMAINS_PROPERTY}' = '\[\"Achats\"\]'")


def test_a_tag_is_recorded_before_it_is_applied(make_project: MakeProject, spark: FakeSpark) -> None:
    """A run that fails halfway does not lose the tags it applied: the next run removes them."""
    api = FakeLakeview()
    dashboard_id = created_once(with_domain(make_project), spark, api)
    api.properties[DOMAINS_PROPERTY] = '["Ventes"]'
    api.tag_api.tags[("dashboards", dashboard_id)] = {"Ventes"}
    api.tag_api.errors["DELETE", "Ventes"] = NotFound("Tag assignment with key Ventes does not exist")
    achats = with_domain(make_project, "Achats", name="achats")

    status, message = run(service_for(achats, spark, api))

    assert status is Status.WARNING
    assert message.endswith(
        "cannot tag it with its Discover domain(s): NotFound: Tag assignment with key Ventes does not exist"
    )
    assert api.tag_api.of("dashboards", dashboard_id) == {"Achats", "Ventes"}
    [recorded] = spark.statements_matching(DOMAINS_PROPERTY)
    assert recorded.endswith(f"('{DOMAINS_PROPERTY}' = '[\"Achats\", \"Ventes\"]')")

    api.properties[DOMAINS_PROPERTY] = '["Achats", "Ventes"]'
    del api.tag_api.errors["DELETE", "Ventes"]
    assert run(service_for(achats, spark, api))[0] is Status.SUCCESS
    assert api.tag_api.of("dashboards", dashboard_id) == {"Achats"}


def test_a_domain_added_in_the_discover_page_is_not_the_services(make_project: MakeProject, spark: FakeSpark) -> None:
    api = FakeLakeview()
    root = with_domain(make_project)
    dashboard_id = created_once(root, spark, api)
    api.tag_api.tags[("dashboards", dashboard_id)] = {"Ventes"}

    assert run(service_for(root, spark, api))[0] is Status.SUCCESS
    assert ("POST", TAG_ASSIGNMENTS) not in api.calls
    assert not spark.statements_matching(DOMAINS_PROPERTY)

    # The contracts no longer expect it: it stays.
    run(service_for(with_domain(make_project, "Achats", name="achats"), spark, api))
    assert api.tag_api.of("dashboards", dashboard_id) == {"Achats", "Ventes"}


@pytest.mark.parametrize("error", [AlreadyExists("exists"), ResourceAlreadyExists("exists")])
def test_a_tag_assigned_meanwhile_is_not_an_error(
    make_project: MakeProject, spark: FakeSpark, error: Exception
) -> None:
    api = FakeLakeview()
    api.tag_api.errors["POST", "Ventes"] = error

    assert run(service_for(with_domain(make_project), spark, api))[0] is Status.SUCCESS


def test_the_tags_are_removed_without_a_domain(root: Path, spark: FakeSpark) -> None:
    api = FakeLakeview()
    dashboard_id = created_once(root, spark, api)
    api.tag_api.tags[("dashboards", dashboard_id)] = {"Ventes"}
    api.properties[DOMAINS_PROPERTY] = '["Ventes"]'

    status, message = run(service_for(root, spark, api))

    assert (status, message.rsplit(", ", 1)[1]) == (Status.SUCCESS, "already published")
    assert api.tag_api.of("dashboards", dashboard_id) == set()
    assert spark.statements_matching(rf"'{DOMAINS_PROPERTY}' = '\[\]'")


def test_a_dashboard_outside_the_discover_environments_leaves_its_domain(
    make_project: MakeProject, spark: FakeSpark
) -> None:
    api = FakeLakeview()
    root = with_domain(make_project)
    dashboard_id = created_once(root, spark, api)
    api.properties[DOMAINS_PROPERTY] = '["Ventes"]'
    api.tag_api.tags[("dashboards", dashboard_id)] = {"Ventes"}
    prod_only = DOMAIN_GROUP.replace("discover_domain=True", 'discover_domain=True, discover_environments=["prod"]')
    dev = make_project(
        {**PROJECT, "sales/sales_config.py": prod_only, "sales/orders/orders_config.py": DASHBOARD_CONFIG},
        name="prod_only",
    )

    status, message = run(service_for(dev, spark, api))

    assert (status, message.rsplit(", ", 1)[1]) == (Status.SUCCESS, "already published")
    assert api.tag_api.of("dashboards", dashboard_id) == set()
    assert spark.statements_matching(rf"'{DOMAINS_PROPERTY}' = '\[\]'")


def test_a_dashboard_that_cannot_be_tagged_is_reported(make_project: MakeProject, spark: FakeSpark) -> None:
    api = FakeLakeview()
    api.tag_api.error = PermissionDenied("User does not have ASSIGN on the tag Ventes")

    status, message = run(service_for(with_domain(make_project), spark, api))

    [dashboard_id] = api.dashboards
    assert status is Status.WARNING
    assert message.endswith(
        "), published; cannot tag it with its Discover domain(s): "
        "PermissionDenied: User does not have ASSIGN on the tag Ventes"
    )
    # The dashboard is recorded, and the next run tags it.
    assert spark.statements_matching(f"'{DASHBOARD_PROPERTY}' = '{dashboard_id}'")
    assert not spark.statements_matching(DOMAINS_PROPERTY)
