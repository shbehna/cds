"""Tests of the metric views generated from the contracts (cds.metric_views), without Spark."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml
from conftest import MakeProject

from cds import ConfigurationError, ContractError, DataContext, MetricViewSettings, ServiceContext
from cds.cli import main
from cds.genie import build_space
from cds.metric_views import MetricView, build_metric_views
from cds.pages import build_pages

GROUP = """
    from cds import GroupConfig, MetadataSettings, MetricViewSettings, PagesSettings

    config = GroupConfig(
        name="sales",
        contract_defaults={"domain": "Ventes"},
        metadata=MetadataSettings(discover_domain=True),
        metric_views=MetricViewSettings(enabled=True, excluded_classifications=["restricted"]),
        pages=PagesSettings(enabled=True),
    )
"""

PRODUCT = """
    from cds import DataProductConfig, GenieSettings

    config = DataProductConfig(name="orders", description="Orders of the shop.", genie=GenieSettings(enabled=True))
"""

CUSTOMER = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.customer
    version: 1.0.0
    schema:
      - name: customer
        businessName: Client
        description: Customers of the shop.
        properties:
          - name: customer_id
            physicalType: STRING
            primaryKey: true
          - name: name
            physicalType: STRING
          - name: email
            physicalType: STRING
            classification: restricted
          - name: segment
            businessName: Segment
            physicalType: STRING
            description: Segment of the customer.
            customProperties:
              - property: synonyms
                value: [Market]
            enum:
              - { value: B2B, label: Business }
              - { value: "B'C", label: "Consumer's" }
          - name: country
            physicalType: STRING
            customProperties:
              - property: allowedValues
                value: { reference: country.code }
"""

COUNTRY = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.country
    version: 1.0.0
    schema:
      - name: country
        properties:
          - name: code
            physicalType: STRING
            primaryKey: true
"""

ORDER = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.order
    version: 1.0.0
    schema:
      - name: order
        description: Orders.
        customProperties:
          - property: measures
            value:
              - name: order_count
                expr: COUNT(1)
                businessName: Number of orders
                description: Number of the orders.
                synonyms: [Orders, Sales]
                format: { type: number }
              - name: revenue
                expr: "SUM(amount)\\n  "
        properties:
          - name: order_id
            physicalType: BIGINT
            primaryKey: true
          - name: customer_id
            businessName: Customer number
            physicalType: STRING
            relationships:
              - to: customer.customer_id
          - name: status
            physicalType: INT
            enum:
              - { value: 1, label: Open }
              - { value: 2, label: Closed }
          - name: priority
            physicalType: STRING
            enum: [high, low]
          - name: amount
            physicalType: DECIMAL(10,2)
"""

LINE = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.line
    version: 1.0.0
    schema:
      - name: line
        customProperties:
          - property: measures
            value:
              - { name: line_count, expr: COUNT(1) }
        properties:
          - name: line_id
            physicalType: BIGINT
            primaryKey: true
          - name: status
            physicalType: INT
            relationships:
              - to: order.status
"""

FILES = {
    "sales/sales_config.py": GROUP,
    "sales/orders/orders_config.py": PRODUCT,
    "sales/orders/customer/customer.yaml": CUSTOMER,
    "sales/orders/country/country.yaml": COUNTRY,
    "sales/orders/order/order.yaml": ORDER,
}


def project(make_project: MakeProject, changes: Mapping[str, str] | None = None, name: str = "products") -> Path:
    return make_project({**FILES, **(changes or {})}, name=name)


def context_of(root: Path, environment: str = "prod") -> ServiceContext:
    return ServiceContext(spark=None, environment=environment, data_context=DataContext.discover(root))


def views_of(root: Path, environment: str = "prod") -> list[MetricView]:
    context = context_of(root, environment)
    return build_metric_views(context.data_context.get_data_product("sales.orders"), context)


def fields_of(view: MetricView) -> dict[str, dict[str, Any]]:
    return {field["name"]: field for field in view.definition["fields"]}


def test_a_table_with_measures_has_a_metric_view(make_project: MakeProject) -> None:
    [view] = views_of(project(make_project))

    assert (view.table.name, view.name) == ("order", "order_metrics")
    definition = view.definition
    assert list(definition) == ["version", "source", "comment", "joins", "fields", "measures"]
    assert (definition["version"], definition["source"], definition["comment"]) == (
        1.1,
        "prod.sales_orders.order",
        "Orders.",
    )
    assert definition["joins"] == [
        {
            "name": "customer",
            "source": "prod.sales_orders.customer",
            "on": "source.`customer_id` = customer.`customer_id`",
        }
    ]
    assert definition["measures"] == [
        {
            "name": "order_count",
            "expr": "COUNT(1)",
            "comment": "Number of the orders.",
            "display_name": "Number of orders",
            "synonyms": ["Orders", "Sales"],
            "format": {"type": "number"},
        },
        # The expression is on one line.
        {"name": "revenue", "expr": "SUM(amount)"},
    ]


def test_the_columns_are_dimensions(make_project: MakeProject) -> None:
    fields = fields_of(views_of(project(make_project))[0])

    assert fields["order_id"] == {"name": "order_id", "expr": "source.`order_id`"}
    assert fields["customer_id"] == {
        "name": "customer_id",
        "expr": "source.`customer_id`",
        "display_name": "Customer number",
    }
    # The labels of an enum are a second dimension; an enum without labels has none.
    assert fields["status_label"]["expr"] == (
        "CASE source.`status` WHEN 1 THEN 'Open' WHEN 2 THEN 'Closed' ELSE CAST(source.`status` AS STRING) END"
    )
    assert "priority_label" not in fields


def test_the_joined_tables_bring_their_keys_and_coded_columns(make_project: MakeProject) -> None:
    fields = fields_of(views_of(project(make_project))[0])
    joined = [name for name, field in fields.items() if "customer.`" in field["expr"]]

    # Not the key of the join, already a column of the source, nor the other columns.
    assert joined == ["customer_segment", "customer_segment_label", "customer_country"]
    assert fields["customer_segment"] == {
        "name": "customer_segment",
        "expr": "customer.`segment`",
        "comment": "Segment of the customer.",
        "display_name": "Segment (code)",
    }
    assert fields["customer_segment_label"] == {
        "name": "customer_segment_label",
        "expr": "CASE customer.`segment` WHEN 'B2B' THEN 'Business' WHEN 'B\\'C' THEN 'Consumer\\'s' "
        "ELSE CAST(customer.`segment` AS STRING) END",
        "comment": "Segment of the customer.",
        "display_name": "Segment",
        "synonyms": ["Market"],
    }


def test_the_excluded_classifications_are_not_dimensions(make_project: MakeProject) -> None:
    measures = (
        "        customProperties:\n"
        "          - property: measures\n"
        "            value:\n"
        "              - { name: customer_count, expr: COUNT(1) }\n"
    )
    customer = CUSTOMER.replace("        properties:\n", measures + "        properties:\n", 1)
    root = project(make_project, {"sales/orders/customer/customer.yaml": customer})
    views = {view.name: view for view in views_of(root)}

    assert "email" not in fields_of(views["customer_metrics"])
    assert "name" in fields_of(views["customer_metrics"])


def test_a_join_to_rows_that_are_not_unique_is_left_out(make_project: MakeProject) -> None:
    views = {view.name: view for view in views_of(project(make_project, {"sales/orders/line/line.yaml": LINE}))}

    assert "joins" not in views["line_metrics"].definition


def test_the_yaml_is_that_of_the_definition(make_project: MakeProject) -> None:
    [view] = views_of(project(make_project))

    assert yaml.safe_load(view.yaml) == view.definition
    # The key on is quoted: unquoted, YAML 1.1 reads it as a boolean.
    assert "'on': source." in view.yaml
    assert view.yaml.startswith("version: 1.1\nsource: prod.sales_orders.order\n")
    assert len(view.digest) == 32
    assert views_of(project(make_project, name="other"))[0].digest == view.digest
    assert views_of(project(make_project, name="dev"), environment="dev")[0].digest != view.digest


def test_the_name_of_the_metric_views_is_configured(make_project: MakeProject) -> None:
    group = GROUP.replace("MetricViewSettings(enabled=True,", 'MetricViewSettings(enabled=True, name="{table}_kpi",')

    assert [view.name for view in views_of(project(make_project, {"sales/sales_config.py": group}))] == ["order_kpi"]


def test_the_metric_views_must_be_enabled(make_project: MakeProject) -> None:
    group = GROUP.replace("MetricViewSettings(enabled=True,", "MetricViewSettings(enabled=False,")

    assert views_of(project(make_project, {"sales/sales_config.py": group})) == []


@pytest.mark.parametrize(
    ("measures", "message"),
    [
        ("COUNT(1)", "must be a list of measures"),
        ("[COUNT(1)]", "measure 1 must be a mapping"),
        ("[{ name: count }]", "measure 1 needs a name and an expr"),
        ("[{ name: count, expr: COUNT(1), unit: rows }]", "unknown key\\(s\\) unit"),
        ("[{ name: count, expr: COUNT(1), format: { type: money } }]", "its format needs a type"),
        ("[{ name: count, expr: COUNT(1), synonyms: [a, b, c, d, e, f, g, h, i, j, k] }]", "at most 10 synonyms"),
        ("[{ name: Status, expr: COUNT(1) }]", "two fields or measures named 'Status'"),
        ("[{ name: count, expr: COUNT(1), description: $$ }]", "cannot contain \\$\\$"),
    ],
)
def test_invalid_measures_are_rejected(make_project: MakeProject, measures: str, message: str) -> None:
    start = ORDER.index("            value:\n              - name: order_count")
    end = ORDER.index("        properties:")
    order = ORDER[:start] + f"            value: {measures}\n" + ORDER[end:]

    with pytest.raises(ContractError, match=rf"sales.orders.order.*{message}"):
        views_of(project(make_project, {"sales/orders/order/order.yaml": order}))


def test_a_field_has_at_most_10_synonyms(make_project: MakeProject) -> None:
    customer = CUSTOMER.replace("value: [Market]", f"value: [{', '.join(f's{index}' for index in range(11))}]")

    with pytest.raises(ContractError, match=r"sales.orders.customer.segment: a field of a metric view has at most 10"):
        views_of(project(make_project, {"sales/orders/customer/customer.yaml": customer}))


def test_a_metric_view_cannot_have_the_name_of_a_table(make_project: MakeProject) -> None:
    group = GROUP.replace("MetricViewSettings(enabled=True,", 'MetricViewSettings(enabled=True, name="{table}",')

    with pytest.raises(ContractError, match=r"its metric view order is the name of a table"):
        views_of(project(make_project, {"sales/sales_config.py": group}))


def test_the_settings_are_checked() -> None:
    with pytest.raises(ConfigurationError, match=r"MetricViewSettings.name must be a pattern with \{table\}"):
        MetricViewSettings(name="metrics")
    with pytest.raises(ConfigurationError, match=r"excluded_classifications must be a list"):
        MetricViewSettings(excluded_classifications="restricted")


def test_the_genie_space_includes_the_metric_views(make_project: MakeProject) -> None:
    root = project(make_project)
    context = context_of(root)
    space = build_space(context.data_context.get_data_product("sales.orders"), context)["serialized_space"]

    identifiers = [source["identifier"] for source in space["data_sources"]["tables"]]
    assert identifiers == sorted(identifiers)
    assert "prod.sales_orders.order_metrics" in identifiers
    text = "".join(space["instructions"]["text_instructions"][0]["content"])
    assert "Metric views, the governed measures of the tables, to query with MEASURE(): order_metrics." in text


def test_the_page_of_a_table_lists_its_measures(make_project: MakeProject) -> None:
    root = project(make_project)
    context = context_of(root)
    pages = {page.key: page for page in build_pages(context.data_context.get_data_product("sales.orders"), context)}

    assert (
        "## Measures\n\nIn the metric view `prod.sales_orders.order_metrics`, to query with `MEASURE()`:\n\n"
        "- *Number of orders* (`order_count`): Number of the orders.\n- *revenue* (`revenue`)"
    ) in pages["table:order"].body
    assert "## Measures" not in pages["table:customer"].body


def test_validate_reports_the_metric_views(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate", str(project(make_project))]) == 0
    assert "  + Metric views: 1 view(s), 2 measure(s): order_metrics\n" in capsys.readouterr().out


def test_validate_reports_invalid_metric_views(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    order = ORDER.replace("expr: COUNT(1)", "expr: ")

    assert main(["validate", str(project(make_project, {"sales/orders/order/order.yaml": order}))]) == 1
    assert "  x Metric views: sales.orders.order: measure 1 needs a name and an expr." in capsys.readouterr().out


def test_the_metric_views_are_printed(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    root = project(make_project)

    assert main(["metrics", str(root), "--data-product", "sales.orders", "--environment", "prod"]) == 0
    printed = capsys.readouterr().out
    assert printed.startswith("# prod.sales_orders.order_metrics\nversion: 1.1\n")


def test_a_data_product_without_metric_views_is_not_printed(project: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["metrics", str(project), "--data-product", "sales.orders"]) == 1
    assert "has no metric view" in capsys.readouterr().err
