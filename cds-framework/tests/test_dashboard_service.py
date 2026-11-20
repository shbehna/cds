"""Tests of the dashboard service, with a fake Lakeview API and a fake Spark session."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from conftest import PROJECT, MakeProject
from databricks.sdk.errors import NotFound
from fakes import FakeSpark
from test_dashboard import DASHBOARD_CONFIG

from cds import DashboardService, DataContext, RuntimeConfiguration, ServiceContext, Status
from cds.dashboard import build_dashboard
from cds.services.dashboard import DASHBOARD_PROPERTY

SCHEMA_PATH = "/api/2.1/unity-catalog/schemas/dev.sales_orders"
DASHBOARDS = "/api/2.0/lakeview/dashboards"


class FakeLakeview:
    """The dashboards and the schema properties of a workspace, as its REST API serves them."""

    def __init__(self, properties: Mapping[str, str] | None = None) -> None:
        self.properties: dict[str, str] = dict(properties or {})
        self.dashboards: dict[str, dict[str, Any]] = {}
        self.published: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str]] = []

    def do(
        self,
        method: str,
        path: str,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls.append((method, path))
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
