"""Tests of the quality dashboard of the demo, without Spark nor workspace."""

import json
import re
from typing import Any

import pytest
import quality_dashboard

from cds import DataContext


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(quality_dashboard.PRODUCTS)


@pytest.fixture(scope="module")
def dashboard(data_context: DataContext) -> dict[str, Any]:
    document: dict[str, Any] = json.loads(quality_dashboard.render(data_context))
    return document


def widgets(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    return [item["widget"] for page in dashboard["pages"] for item in page["layout"]]


def sql(dashboard: dict[str, Any], name: str) -> str:
    [dataset] = [dataset for dataset in dashboard["datasets"] if dataset["name"] == name]
    return "".join(dataset["queryLines"])


def test_the_committed_dashboard_is_up_to_date(data_context: DataContext) -> None:
    committed = quality_dashboard.DASHBOARD.read_text(encoding="utf-8")

    assert committed == quality_dashboard.render(data_context), (
        "The dashboard does not match the contracts: run uv run python cds-demo/dashboards/quality_dashboard.py"
    )


def test_the_dashboard_reads_every_checked_table(dashboard: dict[str, Any]) -> None:
    query = sql(dashboard, "occurrences")

    assert set(re.findall(r"FROM `(\w+)`", query)) == {
        "continuation_transformation",
        "domaine_valeur",
        "entreprise",
        "etablissement",
        "fusion_scission",
        "nom_entreprise",
    }
    # Tables are named without catalog nor schema: the deployment sets them.
    assert not re.search(r"FROM `\w+`\.`", query)


def test_rows_are_identified_by_their_primary_key(dashboard: dict[str, Any]) -> None:
    query = sql(dashboard, "occurrences")

    assert (
        "to_json(named_struct('numero_entreprise', `numero_entreprise`, 'numero_etablissement', "
        "`numero_etablissement`), map('ignoreNullFields', 'false')) AS row_key" in query
    )
    # fusion_scission has no primary key: its rows are identified by all their columns,
    # except the classified ones, which the dashboard would show unmasked.
    assert "'numero_entreprise_liee', `numero_entreprise_liee`" in query
    assert "adresse_ligne" not in query


def test_a_row_with_errors_and_warnings_is_in_error(dashboard: dict[str, Any]) -> None:
    query = sql(dashboard, "row_status")

    assert query.index("size(errors), 0) > 0 THEN 'Error'") < query.index("THEN 'Warning'")


def test_widgets_use_the_fields_of_their_datasets(dashboard: dict[str, Any]) -> None:
    datasets = {dataset["name"]: "".join(dataset["queryLines"]) for dataset in dashboard["datasets"]}
    names = [widget["name"] for widget in widgets(dashboard)]
    assert len(names) == len(set(names)), "widget names must be unique"

    for widget in widgets(dashboard):
        queries = {query["name"]: query["query"] for query in widget.get("queries", [])}
        for query in queries.values():
            assert query["datasetName"] in datasets
            for field in query["fields"]:
                for column in re.findall(r"`(\w+)`", field["expression"]):
                    assert re.search(rf"\b{column}\b", datasets[query["datasetName"]]), (widget["name"], column)
        encodings = widget.get("spec", {}).get("encodings", {})
        references = encodings.get("fields", []) + [
            value for key, value in encodings.items() if key not in ("fields", "columns")
        ]
        for reference in references + encodings.get("columns", []):
            query_name = reference.get("queryName", "main_query")
            assert reference["fieldName"] in {field["name"] for field in queries[query_name]["fields"]}, widget["name"]


def test_the_filters_of_each_page(dashboard: dict[str, Any]) -> None:
    filters = {
        page["name"]: sorted(
            item["widget"]["spec"]["encodings"]["fields"][0]["fieldName"]
            for item in page["layout"]
            if item["widget"].get("spec", {}).get("widgetType") == "filter-multi-select"
        )
        for page in dashboard["pages"]
    }

    assert filters == {"statistics": ["table_name"], "occurrences": ["check_name", "severity", "table_name"]}


def test_the_statistics_page(dashboard: dict[str, Any]) -> None:
    [statistics, _] = dashboard["pages"]
    layout = {item["widget"]["name"]: (item["widget"], item["position"]) for item in statistics["layout"]}

    pie, pie_position = layout["row_status_chart"]
    assert pie["spec"]["widgetType"] == "pie"
    assert (pie_position["x"], pie_position["width"]) == (0, 3), "the pie chart uses the left half"
    bars, _ = layout["occurrences_by_check_chart"]
    assert bars["spec"]["encodings"]["y"]["fieldName"] == "check_name", "horizontal bars: the checks on the y axis"
    table, _ = layout["occurrences_by_table"]
    assert [column["title"] for column in table["spec"]["encodings"]["columns"]] == [
        "Table",
        "Check",
        "Severity",
        "Count",
    ]
    assert pie["spec"]["frame"]["title"] == "Status"
    assert pie["spec"]["encodings"]["angle"]["displayName"] == "Records"
    assert not any("multilineTextboxSpec" in item["widget"] for page in dashboard["pages"] for item in page["layout"])


def test_check_times_are_shown_as_text(dashboard: dict[str, Any]) -> None:
    # The table widget showed the timestamp of the DQX results as epoch milliseconds.
    assert "date_format(result.run_time, 'yyyy-MM-dd HH:mm:ss') AS checked_at" in sql(dashboard, "occurrences")
