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
        "`numero_etablissement`), map('ignoreNullFields', 'false')) AS cle" in query
    )
    # fusion_scission has no primary key: its rows are identified by all their columns.
    assert "'numero_entreprise_liee', `numero_entreprise_liee`" in query


def test_a_row_with_errors_and_warnings_is_in_error(dashboard: dict[str, Any]) -> None:
    query = sql(dashboard, "statut_lignes")

    assert query.index("size(erreurs), 0) > 0 THEN 'Erreur'") < query.index("THEN 'Avertissement'")


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

    assert filters == {"statistiques": ["nom_table"], "occurrences": ["nom_table", "severite", "validation"]}


def test_the_statistics_page(dashboard: dict[str, Any]) -> None:
    [statistics, _] = dashboard["pages"]
    layout = {item["widget"]["name"]: (item["widget"], item["position"]) for item in statistics["layout"]}

    pie, pie_position = layout["statut_des_lignes"]
    assert pie["spec"]["widgetType"] == "pie"
    assert (pie_position["x"], pie_position["width"]) == (0, 3), "the pie chart uses the left half"
    bars, _ = layout["occurrences_par_validation"]
    assert bars["spec"]["encodings"]["x"]["fieldName"] == "validation", "vertical bars: the checks on the x axis"
    table, _ = layout["occurrences_par_table"]
    assert [column["title"] for column in table["spec"]["encodings"]["columns"]] == [
        "Table",
        "Validation",
        "Sévérité",
        "Nombre",
    ]
    assert not any("multilineTextboxSpec" in item["widget"] for page in dashboard["pages"] for item in page["layout"])
