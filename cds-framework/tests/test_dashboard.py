"""Tests of the quality dashboards generated from the contracts (cds.dashboard), without Spark."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
from conftest import PROJECT, MakeProject

from cds import ConfigurationError, DashboardSettings, DataContext, ServiceContext
from cds.cli import main
from cds.dashboard import build_dashboard

DASHBOARD_CONFIG = """
    from cds import DashboardSettings, DataProductConfig

    config = DataProductConfig(
        name="orders",
        description="Orders of the shop.",
        dashboard=DashboardSettings(enabled=True),
    )
"""

CLASSIFIED_CUSTOMER = PROJECT["sales/orders/customer/customer.yaml"].replace(
    "          - name: name\n", "          - name: name\n            classification: restricted\n"
)


def project_with(make_project: MakeProject, **files: str) -> DataContext:
    return DataContext.discover(make_project({**PROJECT, "sales/orders/orders_config.py": DASHBOARD_CONFIG, **files}))


def dashboard_of(data_context: DataContext, environment: str = "dev") -> dict[str, Any]:
    context = ServiceContext(spark=None, environment=environment, data_context=data_context)
    built: dict[str, Any] = build_dashboard(data_context.get_data_product("sales.orders"), context)
    return built


def sql(dashboard: dict[str, Any], name: str) -> str:
    [dataset] = [item for item in dashboard["serialized_dashboard"]["datasets"] if item["name"] == name]
    return "".join(dataset["queryLines"])


def widgets(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    return [item["widget"] for page in dashboard["serialized_dashboard"]["pages"] for item in page["layout"]]


@pytest.fixture
def dashboard(make_project: MakeProject) -> dict[str, Any]:
    return dashboard_of(project_with(make_project), "prod")


def test_the_dashboard_reads_the_checked_tables_of_the_environment(dashboard: dict[str, Any]) -> None:
    assert dashboard["title"] == "sales.orders data quality"
    query = sql(dashboard, "occurrences")
    assert re.findall(r"FROM (`\w+`\.`\w+`\.`\w+`)", query) == [
        "`prod`.`sales_orders`.`customer`",
        "`prod`.`sales_orders`.`order`",
    ]


def test_a_table_without_checks_is_not_in_the_dashboard(make_project: MakeProject) -> None:
    """The quality service skips it: it has no result columns."""
    note = """
        apiVersion: v3.2.0
        kind: DataContract
        id: sales.orders.note
        version: 1.0.0
        schema:
          - name: note
            properties:
              - name: text
                physicalType: STRING
    """
    query = sql(dashboard_of(project_with(make_project, **{"sales/orders/note/note.yaml": note})), "occurrences")

    assert "`note`" not in query


def test_rows_are_identified_by_their_primary_key(dashboard: dict[str, Any]) -> None:
    assert (
        "to_json(named_struct('order_id', `order_id`, 'line', `line`), map('ignoreNullFields', 'false')) AS row_key"
        in sql(dashboard, "occurrences")
    )


def test_the_classified_columns_are_left_out_of_the_keys(make_project: MakeProject) -> None:
    no_key = CLASSIFIED_CUSTOMER.replace("            primaryKey: true\n", "")
    data_context = project_with(make_project, **{"sales/orders/customer/customer.yaml": no_key})

    query = sql(dashboard_of(data_context), "occurrences")

    # Without primary key, the rows are identified by all their columns but the restricted name.
    assert "named_struct('customer_id', `customer_id`, 'created_on', `created_on`)" in query
    shown = DASHBOARD_CONFIG.replace("enabled=True", 'enabled=True, shown_classifications=["public", "restricted"]')
    data_context = project_with(
        make_project, **{"sales/orders/customer/customer.yaml": no_key, "sales/orders/orders_config.py": shown}
    )
    assert "'name', `name`" in sql(dashboard_of(data_context), "occurrences")


def test_a_key_of_classified_columns_only_fails(make_project: MakeProject) -> None:
    customer = CLASSIFIED_CUSTOMER.replace(
        "          - name: customer_id\n", "          - name: customer_id\n            classification: restricted\n"
    )
    data_context = project_with(make_project, **{"sales/orders/customer/customer.yaml": customer})

    with pytest.raises(ConfigurationError, match="every column of its key is classified"):
        dashboard_of(data_context)


def test_a_dashboard_needs_a_checked_table(make_project: MakeProject) -> None:
    config = DASHBOARD_CONFIG.replace("    dashboard=", "    quality=QualitySettings(enabled=False),\n    dashboard=")
    config = config.replace("from cds import DashboardSettings,", "from cds import DashboardSettings, QualitySettings,")
    data_context = project_with(make_project, **{"sales/orders/orders_config.py": config})

    with pytest.raises(ConfigurationError, match="no table is checked by the quality service"):
        dashboard_of(data_context)


def test_a_row_with_errors_and_warnings_is_in_error(dashboard: dict[str, Any]) -> None:
    query = sql(dashboard, "row_status")

    assert query.index("size(errors), 0) > 0 THEN 'Error'") < query.index("THEN 'Warning'")


def test_widgets_use_the_fields_of_their_datasets(dashboard: dict[str, Any]) -> None:
    datasets = {item["name"]: "".join(item["queryLines"]) for item in dashboard["serialized_dashboard"]["datasets"]}
    names = [widget["name"] for widget in widgets(dashboard)]
    assert len(names) == len(set(names)), "widget names must be unique"

    for widget in widgets(dashboard):
        queries = {item["name"]: item["query"] for item in widget.get("queries", [])}
        for item in queries.values():
            assert item["datasetName"] in datasets
            for field in item["fields"]:
                for column in re.findall(r"`(\w+)`", field["expression"]):
                    assert re.search(rf"\b{column}\b", datasets[item["datasetName"]]), (widget["name"], column)
        encodings = widget.get("spec", {}).get("encodings", {})
        references = encodings.get("fields", []) + [
            value for key, value in encodings.items() if key not in ("fields", "columns")
        ]
        for reference in references + encodings.get("columns", []):
            query_name = reference.get("queryName", "main_query")
            assert reference["fieldName"] in {field["name"] for field in queries[query_name]["fields"]}, widget["name"]


def test_the_pages_and_their_filters(dashboard: dict[str, Any]) -> None:
    filters = {
        page["name"]: sorted(
            item["widget"]["spec"]["encodings"]["fields"][0]["fieldName"]
            for item in page["layout"]
            if item["widget"].get("spec", {}).get("widgetType") == "filter-multi-select"
        )
        for page in dashboard["serialized_dashboard"]["pages"]
    }

    assert filters == {"statistics": ["table_name"], "occurrences": ["check_name", "severity", "table_name"]}
    # The table widget showed the timestamp of the DQX results as epoch milliseconds.
    assert "date_format(result.run_time, 'yyyy-MM-dd HH:mm:ss') AS checked_at" in sql(dashboard, "occurrences")


def test_the_title_of_the_settings(make_project: MakeProject) -> None:
    config = DASHBOARD_CONFIG.replace("enabled=True", 'enabled=True, title="Orders quality"')
    data_context = project_with(make_project, **{"sales/orders/orders_config.py": config})

    assert dashboard_of(data_context)["title"] == "Orders quality"


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"enabled": "yes"}, r"DashboardSettings\.enabled must be a boolean"),
        ({"title": ""}, r"DashboardSettings\.title must be a non-empty string"),
        ({"shown_classifications": "public"}, r"DashboardSettings\.shown_classifications must be a list"),
    ],
)
def test_the_settings_are_validated(values: dict[str, Any], message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        DashboardSettings(**values)


def test_a_data_product_has_no_dashboard_by_default(project: Path) -> None:
    data_product = DataContext.discover(project).get_data_product("sales.orders")

    assert data_product.settings(DashboardSettings).enabled is False


def test_cli_dashboard(make_project: MakeProject, project: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = make_project({**PROJECT, "sales/orders/orders_config.py": DASHBOARD_CONFIG}, name="with_dashboard")

    assert main(["dashboard", str(root), "--data-product", "sales.orders", "--environment", "nprod"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert "`nprod`.`sales_orders`.`order`" in "".join(printed["serialized_dashboard"]["datasets"][0]["queryLines"])

    assert main(["validate", str(root)]) == 0
    assert "+ Dashboard: 2 table(s), 2 page(s)" in capsys.readouterr().out

    assert main(["dashboard", str(project), "--data-product", "sales.orders"]) == 1
    assert "sales.orders has no dashboard: enable it with DashboardSettings(enabled=True)" in capsys.readouterr().err
