"""Tests of the metrics service, with a fake Spark session keeping the metric views of a schema."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from conftest import MakeProject
from fakes import FakeSpark, Row
from test_metric_views import FILES, GROUP, ORDER

from cds import DataContext, MetricsService, ServiceContext, Status
from cds.metric_views import build_metric_views
from cds.services.metrics import DIGEST_PROPERTY, SOURCE_PROPERTY

SCHEMA = "`prod`.`sales_orders`"
VIEW = "prod.sales_orders.order_metrics"


def _unquote(text: str) -> str:
    return re.sub(r"\\(.)", r"\1", text)


class FakeCatalog:
    """The tables, metric views, properties and tags of a schema, kept from the statements of the service."""

    def __init__(self, spark: FakeSpark) -> None:
        self.tables = {"customer", "country", "order"}
        self.views: dict[str, dict[str, str]] = {}
        self.yaml: dict[str, str] = {}
        self.tags: dict[str, dict[str, str]] = {}
        spark.on(rf"^SHOW TABLES IN {re.escape(SCHEMA)}$", self._show_tables)
        spark.on(r"information_schema`\.`tables`", lambda _: [Row(table_name=name) for name in sorted(self.views)])
        spark.on(r"^SHOW TBLPROPERTIES ", self._properties)
        spark.on(r"information_schema`\.`table_tags`", self._tags)
        spark.on(r"^CREATE VIEW ", self._create)
        spark.on(r"^ALTER VIEW \S+ AS ", self._alter)
        spark.on(r"^ALTER VIEW \S+ SET TBLPROPERTIES ", self._set_properties)
        spark.on(r"^ALTER VIEW \S+ (UN)?SET TAGS ", self._set_tags)
        spark.on(r"^DROP VIEW ", self._drop)

    @staticmethod
    def _name(statement: str) -> str:
        match = re.search(r"`prod`\.`sales_orders`\.`(\w+)`", statement)
        assert match is not None, statement
        return match.group(1)

    def _show_tables(self, _: str) -> list[Row]:
        return [Row(tableName=name, isTemporary=False) for name in sorted(self.tables | set(self.views))]

    def _properties(self, statement: str) -> list[Row]:
        return [Row(key=key, value=value) for key, value in self.views[self._name(statement)].items()]

    def _tags(self, _: str) -> list[Row]:
        return [
            Row(table_name=name, tag_name=key, tag_value=value)
            for name, tags in self.tags.items()
            for key, value in tags.items()
        ]

    def _create(self, statement: str) -> list[Row]:
        name = self._name(statement)
        assert name not in self.views and name not in self.tables, "CREATE VIEW would fail"
        properties = re.search(r"TBLPROPERTIES \((.*?)\) AS \$\$", statement)
        assert properties is not None
        self.views[name] = dict(re.findall(r"'((?:[^'\\]|\\.)*)' = '((?:[^'\\]|\\.)*)'", properties.group(1)))
        self.yaml[name] = statement.split("$$")[1]
        return []

    def _alter(self, statement: str) -> list[Row]:
        self.yaml[self._name(statement)] = statement.split("$$")[1]
        return []

    def _set_properties(self, statement: str) -> list[Row]:
        for key, value in re.findall(r"'((?:[^'\\]|\\.)*)' = '((?:[^'\\]|\\.)*)'", statement):
            self.views[self._name(statement)][_unquote(key)] = _unquote(value)
        return []

    def _set_tags(self, statement: str) -> list[Row]:
        tags = self.tags.setdefault(self._name(statement), {})
        if " UNSET TAGS " in statement:
            for key in re.findall(r"'((?:[^'\\]|\\.)*)'", statement.split(" UNSET TAGS ")[1]):
                tags.pop(_unquote(key))
        else:
            for key, value in re.findall(r"'((?:[^'\\]|\\.)*)' = '((?:[^'\\]|\\.)*)'", statement):
                tags[_unquote(key)] = _unquote(value)
        return []

    def _drop(self, statement: str) -> list[Row]:
        name = self._name(statement)
        self.views.pop(name)
        self.tags.pop(name, None)
        return []


def service_for(root: Path, spark: FakeSpark, environment: str = "prod") -> MetricsService:
    context = ServiceContext(spark=spark.as_session(), environment=environment, data_context=DataContext.discover(root))
    return MetricsService(context)


def results(service: MetricsService, tables: list[str] | None = None) -> dict[str, tuple[Status, str]]:
    data_product = service.context.data_context.get_data_product("sales.orders")
    report = service.run(data_product, tables=tables)
    return {result.table: (result.status, result.message) for result in report.results}


@pytest.fixture
def root(make_project: MakeProject) -> Path:
    group = GROUP.replace(
        "metadata=MetadataSettings(discover_domain=True),",
        'metadata=MetadataSettings(discover_domain=True, discover_environments=["prod"], '
        'certification={"active": "certified"}),',
    )
    order = ORDER.replace("version: 1.0.0\n", "version: 1.0.0\n    status: active\n")
    return make_project({**FILES, "sales/sales_config.py": group, "sales/orders/order/order.yaml": order})


@pytest.fixture
def catalog(spark: FakeSpark) -> FakeCatalog:
    return FakeCatalog(spark)


def test_the_metric_view_is_created_and_tagged(root: Path, spark: FakeSpark, catalog: FakeCatalog) -> None:
    service = service_for(root, spark)

    found = results(service)

    assert found["sales.orders.order"] == (
        Status.SUCCESS,
        f"created the metric view {VIEW}: 9 dimension(s), 2 measure(s), 1 join(s), 1 tag statement(s)",
    )
    assert found["sales.orders.customer"] == (Status.SKIPPED, "no metric view: the contract declares no measures")
    [view] = build_metric_views(service.context.data_context.get_data_product("sales.orders"), service.context)
    assert catalog.views["order_metrics"] == {SOURCE_PROPERTY: "order", DIGEST_PROPERTY: view.digest}
    assert catalog.yaml["order_metrics"] == f"\n{view.yaml}"
    assert catalog.tags["order_metrics"] == {"system.certification_status": "certified", "Ventes": ""}
    [create] = spark.statements_matching(r"^CREATE VIEW")
    assert create.startswith("CREATE VIEW `prod`.`sales_orders`.`order_metrics` WITH METRICS LANGUAGE YAML ")


def test_an_unchanged_metric_view_is_left_as_it_is(root: Path, spark: FakeSpark, catalog: FakeCatalog) -> None:
    results(service_for(root, spark))
    spark.statements.clear()

    found = results(service_for(root, spark))

    assert found["sales.orders.order"][1].startswith(f"unchanged the metric view {VIEW}")
    assert spark.statements_matching(r"^(CREATE|ALTER|DROP)") == []


def test_a_changed_metric_view_is_updated_in_place(
    root: Path, spark: FakeSpark, catalog: FakeCatalog, make_project: MakeProject
) -> None:
    results(service_for(root, spark))
    spark.statements.clear()
    order = (root / "sales/orders/order/order.yaml").read_text(encoding="utf-8").replace("COUNT(1)", "COUNT(*)")
    changed = make_project(
        {**{path: (root / path).read_text(encoding="utf-8") for path in FILES}, "sales/orders/order/order.yaml": order},
        name="changed",
    )
    service = service_for(changed, spark)

    found = results(service)

    assert found["sales.orders.order"][1].startswith(f"updated the metric view {VIEW}")
    assert [statement.split(" AS ")[0] for statement in spark.statements_matching(r"^ALTER VIEW \S+ AS ")] == [
        "ALTER VIEW `prod`.`sales_orders`.`order_metrics`"
    ]
    [view] = build_metric_views(service.context.data_context.get_data_product("sales.orders"), service.context)
    assert catalog.views["order_metrics"][DIGEST_PROPERTY] == view.digest
    assert "COUNT(*)" in catalog.yaml["order_metrics"]
    # Updated in place, the view keeps its tags.
    assert spark.statements_matching(r"SET TAGS") == []


def test_an_object_of_the_same_name_is_not_replaced(root: Path, spark: FakeSpark, catalog: FakeCatalog) -> None:
    catalog.tables.add("order_metrics")

    status, message = results(service_for(root, spark))["sales.orders.order"]

    assert (status, message) == (
        Status.ERROR,
        f"{VIEW} exists and is not the metric view of the table: rename one",
    )
    assert spark.statements_matching(r"^CREATE") == []


def test_a_metric_view_of_another_table_is_not_replaced(root: Path, spark: FakeSpark, catalog: FakeCatalog) -> None:
    catalog.views["order_metrics"] = {SOURCE_PROPERTY: "customer"}

    assert results(service_for(root, spark))["sales.orders.order"][0] is Status.ERROR


def test_the_metric_views_no_longer_declared_are_dropped(
    root: Path, spark: FakeSpark, catalog: FakeCatalog, make_project: MakeProject
) -> None:
    results(service_for(root, spark))
    # A metric view created by another tool is left untouched.
    catalog.views["other_metrics"] = {"owner": "someone"}
    order = (root / "sales/orders/order/order.yaml").read_text(encoding="utf-8")
    start, end = order.index("    customProperties:"), order.index("    properties:")
    files = {path: (root / path).read_text(encoding="utf-8") for path in FILES}
    removed = make_project({**files, "sales/orders/order/order.yaml": order[:start] + order[end:]}, name="removed")

    found = results(service_for(removed, spark))

    assert found[VIEW] == (Status.SUCCESS, f"dropped the metric view {VIEW}: no longer declared")
    assert set(catalog.views) == {"other_metrics"}


def test_a_run_on_some_tables_drops_nothing(
    root: Path, spark: FakeSpark, catalog: FakeCatalog, make_project: MakeProject
) -> None:
    catalog.views["stale_metrics"] = {SOURCE_PROPERTY: "stale"}

    found = results(service_for(root, spark), tables=["order"])

    assert list(found) == ["sales.orders.order"]
    assert "stale_metrics" in catalog.views


def test_outside_the_discover_environments_the_view_leaves_its_domain(
    root: Path, spark: FakeSpark, catalog: FakeCatalog
) -> None:
    results(service_for(root, spark))
    service = service_for(root, spark, environment="dev")
    # The views of dev, in the same schema for the test.
    service.context.full_schema_name = lambda element: "prod.sales_orders"  # type: ignore[method-assign]

    results(service)

    assert catalog.tags["order_metrics"] == {"system.certification_status": "certified"}


def test_the_tags_follow_the_metadata_settings(
    root: Path, spark: FakeSpark, catalog: FakeCatalog, make_project: MakeProject
) -> None:
    files = {path: (root / path).read_text(encoding="utf-8") for path in FILES}
    group = files["sales/sales_config.py"].replace("MetadataSettings(", "MetadataSettings(tags=False, ")
    untagged = make_project({**files, "sales/sales_config.py": group}, name="untagged")

    results(service_for(untagged, spark))

    assert catalog.tags.get("order_metrics", {}) == {}


def test_the_service_is_skipped_without_metric_views(
    make_project: MakeProject, spark: FakeSpark, catalog: FakeCatalog
) -> None:
    group = GROUP.replace("MetricViewSettings(enabled=True,", "MetricViewSettings(enabled=False,")
    root = make_project({**FILES, "sales/sales_config.py": group})

    found = results(service_for(root, spark))

    assert found["sales.orders.order"] == (Status.SKIPPED, "no metric view: MetricViewSettings.enabled is not set")
    assert catalog.views == {}
