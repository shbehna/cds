"""Tests of the pages service, with a fake Pages API and a fake Spark session."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from conftest import MakeProject
from databricks.sdk.errors import AlreadyExists, NotFound
from fakes import FakeSpark, Row
from test_pages import CUSTOMER, FILES, PRODUCT

from cds import DataContext, PagesService, RuntimeConfiguration, ServiceContext, Status
from cds.pages import build_pages
from cds.services.pages import PAGES_PROPERTY, changed_fields, page_body, recorded_pages

SCHEMA_PATH = "/api/2.1/unity-catalog/schemas/prod.sales_orders"
DOMAIN = "domains/0001"


class FakeApi:
    """The Discover domains, the Pages and the schema properties of a workspace, as its REST API serves them.

    The properties set with ``ALTER SCHEMA`` by the fake Spark session are recorded too.
    """

    def __init__(self, spark: FakeSpark) -> None:
        self.properties: dict[str, str] = {}
        self.domains: list[dict[str, Any]] = [
            {"name": "domains/system", "tag_key": "system.general_domain"},
            {"name": DOMAIN, "tag_key": "Ventes"},
        ]
        self.pages: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str]] = []
        self.patches: list[tuple[str, Mapping[str, str] | None, Mapping[str, Any]]] = []
        # Titles of the published Pages of other data products, by domain.
        self.taken: set[tuple[str, str]] = set()
        self.created = 0
        spark.on(r"^ALTER SCHEMA", self._alter_schema)

    def _alter_schema(self, statement: str) -> list[Row]:
        match = re.search(r"DBPROPERTIES \('([^']+)' = '((?:[^'\\]|\\.)*)'\)$", statement)
        assert match is not None, statement
        self.properties[match.group(1)] = re.sub(r"\\(.)", r"\1", match.group(2))
        return []

    def recorded(self) -> dict[str, str]:
        return recorded_pages(self.properties.get(PAGES_PROPERTY))

    def do(
        self,
        method: str,
        path: str,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls.append((method, path))
        if path == SCHEMA_PATH and method == "GET":
            return {"full_name": "prod.sales_orders", "properties": dict(self.properties)}
        if path == "/api/2.0/domains" and method == "GET":
            # Two pages of results.
            if (query or {}).get("page_token") == "next":
                return {"domains": self.domains[1:]}
            return {"domains": self.domains[:1], "next_page_token": "next"}
        if path == "/api/2.0/knowledge/pages" and method == "POST":
            page = dict(body or {})
            self._check_title(page)
            self.created += 1
            name = f"pages/{self.created:04d}"
            # The API sorts the synonyms, and sets the owner.
            self.pages[name] = {**page, "name": name, "synonyms": sorted(page["synonyms"]), "owner_ids": ["1"]}
            return dict(self.pages[name])
        name = path.removeprefix("/api/2.0/knowledge/")
        if name not in self.pages:
            raise NotFound(f"Knowledge page with id '{name}' does not exist")
        if method == "GET":
            return dict(self.pages[name])
        if method == "PATCH":
            mask = (query or {})["update_mask"].split(",")
            assert set(mask) == set(body or {}), "the mask must list the fields of the request"
            self._check_title({**self.pages[name], **(body or {})})
            self.patches.append((name, query, dict(body or {})))
            self.pages[name].update(body or {})
            return dict(self.pages[name])
        if method == "DELETE":
            del self.pages[name]
            return {}
        raise AssertionError(f"unexpected call {method} {path}")

    def _check_title(self, page: Mapping[str, Any]) -> None:
        if page["state"] == "PUBLISHED" and (page["domain"], page["display_name"]) in self.taken:
            raise AlreadyExists("A published page with this title already exists in the domain")


def service_for(root: Path, spark: FakeSpark, api: FakeApi, environment: str = "prod") -> PagesService:
    client = MagicMock()
    client.api_client = api
    context = ServiceContext(
        spark=spark.as_session(),
        environment=environment,
        data_context=DataContext.discover(root),
        runtime_configuration=RuntimeConfiguration({"environment": environment}),
        workspace_client=client,
    )
    return PagesService(context)


def run(service: PagesService) -> tuple[Status, str]:
    data_product = service.context.data_context.get_data_product("sales.orders")
    [result] = service.run(data_product).results
    return result.status, result.message


@pytest.fixture
def root(make_project: MakeProject) -> Path:
    return make_project(FILES)


@pytest.fixture
def api(spark: FakeSpark) -> FakeApi:
    return FakeApi(spark)


def by_title(api: FakeApi) -> dict[str, dict[str, Any]]:
    return {page["display_name"]: page for page in api.pages.values()}


def test_the_pages_are_created_and_recorded_in_the_schema(root: Path, spark: FakeSpark, api: FakeApi) -> None:
    service = service_for(root, spark, api)

    status, message = run(service)

    assert (status, message) == (Status.SUCCESS, "4 Page(s): 4 created, in the Discover domain(s) Ventes")
    data_product = service.context.data_context.get_data_product("sales.orders")
    expected = build_pages(data_product, service.context)
    assert sorted(by_title(api)) == sorted(page.display_name for page in expected)
    client = by_title(api)["Client"]
    assert client == {
        **page_body(expected[0], DOMAIN),
        "name": client["name"],
        "owner_ids": ["1"],
    }
    assert client["state"] == "PUBLISHED"
    assert api.recorded() == {page.key: page_name for page_name, page in zip(api.pages, expected, strict=True)}


def test_unchanged_pages_are_not_updated(root: Path, spark: FakeSpark, api: FakeApi) -> None:
    run(service_for(root, spark, api))
    spark.statements.clear()
    api.calls.clear()

    status, message = run(service_for(root, spark, api))

    assert (status, message) == (Status.SUCCESS, "4 Page(s): 4 unchanged, in the Discover domain(s) Ventes")
    assert {method for method, _ in api.calls} == {"GET"}
    assert spark.statements == []


def test_only_the_changed_fields_are_updated(
    root: Path, spark: FakeSpark, api: FakeApi, make_project: MakeProject
) -> None:
    run(service_for(root, spark, api))
    customer = CUSTOMER.replace("description: Customers of the shop.", "description: Buyers of the shop.").replace(
        '[" Buyer ", Account, Buyer]', "[Account]"
    )
    changed = make_project({**FILES, "sales/orders/customer/customer.yaml": customer}, name="changed")

    status, message = run(service_for(changed, spark, api))

    assert (status, message.split(", in ")[0]) == (Status.SUCCESS, "4 Page(s): 1 updated, 3 unchanged")
    [(name, query, body)] = api.patches
    assert by_title(api)["Client"]["name"] == name
    assert query == {"update_mask": "description,body,synonyms"}
    assert body["description"] == "Buyers of the shop."
    assert body["synonyms"] == ["Account"]


def test_drafts_are_published_once_in_a_discover_environment(root: Path, spark: FakeSpark, api: FakeApi) -> None:
    # The drafts of an environment outside discover_environments, in the same schema for the test.
    service = service_for(root, spark, api, environment="dev")
    service.context.full_schema_name = lambda element: "prod.sales_orders"  # type: ignore[method-assign]
    status, message = run(service)
    assert (status, message.rsplit(" (", 1)[1]) == (Status.SUCCESS, "4 draft(s))")
    assert {page["state"] for page in api.pages.values()} == {"DRAFT"}

    run(service_for(root, spark, api))

    assert {page["state"] for page in api.pages.values()} == {"PUBLISHED"}
    assert {query["update_mask"] for _, query, _ in api.patches if query} == {"state"}


def test_a_deleted_page_is_created_again(root: Path, spark: FakeSpark, api: FakeApi) -> None:
    run(service_for(root, spark, api))
    deleted = by_title(api)["Client"]["name"]
    del api.pages[deleted]

    status, message = run(service_for(root, spark, api))

    assert (status, message.split(", in ")[0]) == (Status.SUCCESS, "4 Page(s): 1 created, 3 unchanged")
    assert by_title(api)["Client"]["name"] != deleted
    assert by_title(api)["Client"]["name"] == api.recorded()["table:customer"]


def test_the_pages_no_longer_described_are_deleted(
    root: Path, spark: FakeSpark, api: FakeApi, make_project: MakeProject
) -> None:
    run(service_for(root, spark, api))
    without = {key: value for key, value in FILES.items() if "customer" not in key}
    order = FILES["sales/orders/order/order.yaml"]
    without["sales/orders/order/order.yaml"] = order.replace(
        "            relationships:\n              - to: customer.customer_id\n", ""
    ).replace("          - name: channel\n", "          - name: channel_removed\n")
    removed = make_project(without, name="removed")
    # The channel column checks its values against the removed customer table.
    removed_order = removed / "sales/orders/order/order.yaml"
    removed_order.write_text(
        removed_order.read_text(encoding="utf-8").replace("customer.segment", "order.status"), encoding="utf-8"
    )

    status, message = run(service_for(removed, spark, api))

    assert (status, message.split(", in ")[0]) == (Status.SUCCESS, "3 Page(s): 2 updated, 1 unchanged, 1 deleted")
    assert sorted(by_title(api)) == ["Order status", "Segment", "order"]
    assert sorted(api.recorded()) == ["code_list:Ventes:order status", "code_list:Ventes:segment", "table:order"]


def test_all_the_pages_are_deleted_without_discover_domain(
    root: Path, spark: FakeSpark, api: FakeApi, make_project: MakeProject
) -> None:
    run(service_for(root, spark, api))
    group = FILES["sales/sales_config.py"].replace("discover_domain=True", "discover_domain=False")
    without = make_project({**FILES, "sales/sales_config.py": group}, name="without")

    status, message = run(service_for(without, spark, api))

    assert (status, message) == (Status.SUCCESS, "0 Page(s): 4 deleted")
    assert api.pages == {}
    assert api.recorded() == {}
    assert run(service_for(without, spark, api)) == (
        Status.SKIPPED,
        "no Pages: no table is in a Discover domain (MetadataSettings.discover_domain)",
    )


def test_a_page_already_deleted_is_forgotten(
    root: Path, spark: FakeSpark, api: FakeApi, make_project: MakeProject
) -> None:
    run(service_for(root, spark, api))
    api.pages.clear()
    group = FILES["sales/sales_config.py"].replace("discover_domain=True", "discover_domain=False")

    status, _ = run(service_for(make_project({**FILES, "sales/sales_config.py": group}, name="without"), spark, api))

    assert status is Status.SUCCESS
    assert api.recorded() == {}


def test_a_missing_domain_is_a_warning(root: Path, spark: FakeSpark, api: FakeApi) -> None:
    api.domains = api.domains[:1]

    status, message = run(service_for(root, spark, api))

    assert status is Status.WARNING
    assert message.startswith("4 Page(s): none, in the Discover domain(s) Ventes; 'Client': the Discover domain")
    assert api.pages == {}


def test_a_title_taken_in_the_domain_is_a_warning(root: Path, spark: FakeSpark, api: FakeApi) -> None:
    api.taken.add((DOMAIN, "Segment"))

    status, message = run(service_for(root, spark, api))

    assert status is Status.WARNING
    assert message == (
        "4 Page(s): 3 created, in the Discover domain(s) Ventes; "
        "'Segment': another published Page of 'Ventes' has this title"
    )
    assert "Segment" not in by_title(api)


def test_a_page_whose_id_cannot_be_recorded_is_deleted(root: Path, spark: FakeSpark, api: FakeApi) -> None:
    """Otherwise the next run would not find it, and would create another one."""
    spark.on(r"^ALTER SCHEMA", RuntimeError("permission denied"))

    status, message = run(service_for(root, spark, api))

    assert (status, message) == (Status.ERROR, "RuntimeError: permission denied")
    assert api.pages == {}
    assert ("DELETE", "/api/2.0/knowledge/pages/0001") in api.calls


def test_the_service_is_skipped_without_pages(make_project: MakeProject, spark: FakeSpark, api: FakeApi) -> None:
    product = PRODUCT.replace("PagesSettings(enabled=True)", "PagesSettings(enabled=False)")
    root = make_project({**FILES, "sales/orders/orders_config.py": product})

    assert run(service_for(root, spark, api)) == (Status.SKIPPED, "no Pages: PagesSettings.enabled is not set")
    assert api.calls == []


def test_the_recorded_pages_are_read_defensively() -> None:
    assert recorded_pages(None) == {}
    assert recorded_pages("not json") == {}
    assert recorded_pages('["pages/1"]') == {}
    assert recorded_pages(json.dumps({"table:a": "pages/1", "table:b": "../domains/1", "table:c": 3})) == {
        "table:a": "pages/1"
    }


def test_the_synonyms_are_compared_as_sets() -> None:
    body = {"domain": DOMAIN, "display_name": "A", "description": "", "body": "b", "synonyms": ["b", "a"]}
    body["state"] = "DRAFT"

    assert changed_fields({**body, "synonyms": ["a", "b"]}, body) == []
    # A missing description is the empty description of the request.
    assert changed_fields({key: value for key, value in body.items() if key != "description"}, body) == []
    assert changed_fields({**body, "synonyms": ["a"], "state": "PUBLISHED"}, body) == ["synonyms", "state"]
