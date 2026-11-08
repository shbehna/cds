"""Generates the quality dashboard of the REQ data product, a Databricks AI/BI dashboard.

The dashboard shows the results of the quality service: the DQX errors and warnings
written in each row of the tables. Its queries are generated from the data contracts,
which give the tables, their primary keys and the result columns of DQX.

The queries name the tables without catalog nor schema: the deployment sets them for
each environment (``dataset_catalog`` and ``dataset_schema`` of the dashboard). The
generated file is committed; regenerate it, from the root of the repository, after
changing the contracts:

    uv run python cds-demo/dashboards/quality_dashboard.py
"""

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cds import DataContext, QualitySettings, Table
from cds.config import resolved
from cds.sql import quote_identifier, string_literal

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
DASHBOARD = Path(__file__).with_name("qualite_req.lvdash.json")
DATA_PRODUCT = "donnees_qc.req"

# Status of a row: a row with errors and warnings is in error.
ERROR = "Erreur"
WARNING = "Avertissement"
OK = "OK"


def checked_tables(data_context: DataContext) -> list[Table]:
    """Returns the tables of the data product checked by the quality service."""
    tables = data_context.get_data_product(DATA_PRODUCT).tables
    return sorted((table for table in tables if table.settings(QualitySettings).enabled), key=lambda t: t.name)


def key_expression(table: Table) -> str:
    """SQL expression of the key of a row, as a JSON object: column name to value.

    The key is the primary key of the contract; rows of a table without primary key are
    identified by all their columns.
    """
    columns = list(table.primary_key) or [prop.column for prop in table.columns]
    pairs = ", ".join(f"{string_literal(column)}, {quote_identifier(column)}" for column in columns)
    # Null values are kept: the key shows all its columns.
    return f"to_json(named_struct({pairs}), map('ignoreNullFields', 'false'))"


def rows_query(tables: Sequence[Table]) -> str:
    """Query of the rows of all the tables: their table, key, errors and warnings."""
    selects = []
    for table in tables:
        settings = table.settings(QualitySettings)
        selects.append(
            f"  SELECT {string_literal(table.name)} AS nom_table, {key_expression(table)} AS cle,\n"
            f"    {quote_identifier(resolved(settings.errors_column))} AS erreurs,\n"
            f"    {quote_identifier(resolved(settings.warnings_column))} AS avertissements\n"
            f"  FROM {quote_identifier(table.name)}"
        )
    return "WITH lignes AS (\n" + "\n  UNION ALL\n".join(selects) + "\n)"


def row_status_query(tables: Sequence[Table]) -> str:
    """Number of rows of each table without issue, with warnings only, and with errors."""
    return (
        f"{rows_query(tables)},\n"
        "statuts AS (\n"
        "  SELECT nom_table,\n"
        f"    CASE WHEN coalesce(size(erreurs), 0) > 0 THEN '{ERROR}'\n"
        f"      WHEN coalesce(size(avertissements), 0) > 0 THEN '{WARNING}'\n"
        f"      ELSE '{OK}' END AS statut\n"
        "  FROM lignes\n"
        ")\n"
        "SELECT nom_table,\n"
        "  count(*) AS lignes,\n"
        f"  count_if(statut = '{OK}') AS lignes_ok,\n"
        f"  count_if(statut = '{WARNING}') AS lignes_avertissement,\n"
        f"  count_if(statut = '{ERROR}') AS lignes_erreur\n"
        "FROM statuts\n"
        "GROUP BY nom_table"
    )


def _occurrences(tables: Sequence[Table]) -> str:
    return (
        f"{rows_query(tables)},\n"
        "occurrences AS (\n"
        f"  SELECT nom_table, cle, '{ERROR}' AS niveau, resultat\n"
        "  FROM lignes LATERAL VIEW explode(erreurs) e AS resultat\n"
        "  UNION ALL\n"
        f"  SELECT nom_table, cle, '{WARNING}' AS niveau, resultat\n"
        "  FROM lignes LATERAL VIEW explode(avertissements) a AS resultat\n"
        ")\n"
    )


def occurrences_query(tables: Sequence[Table]) -> str:
    """Each error and warning of each row, with the key of the row."""
    return (
        f"{_occurrences(tables)}"
        "SELECT nom_table, cle, niveau,\n"
        "  resultat.name AS validation,\n"
        "  resultat.function AS fonction,\n"
        "  array_join(resultat.columns, ', ') AS colonnes,\n"
        "  resultat.message AS message,\n"
        "  resultat.run_time AS date_verification\n"
        "FROM occurrences"
    )


def occurrences_by_check_query(tables: Sequence[Table]) -> str:
    """Number of errors and warnings of each check, by table."""
    return (
        f"{_occurrences(tables)}"
        "SELECT nom_table, niveau, resultat.name AS validation, count(*) AS occurrences\n"
        "FROM occurrences\n"
        "GROUP BY nom_table, niveau, resultat.name"
    )


def dataset(name: str, display_name: str, query: str) -> dict[str, Any]:
    lines = query.split("\n")
    return {
        "name": name,
        "displayName": display_name,
        "queryLines": [line + "\n" for line in lines[:-1]] + lines[-1:],
    }


def field(name: str, expression: str | None = None) -> dict[str, str]:
    return {"name": name, "expression": expression or f"`{name}`"}


def query(
    dataset_name: str, fields: Sequence[dict[str, str]], *, disaggregated: bool, name: str = "main_query"
) -> dict[str, Any]:
    return {
        "name": name,
        "query": {"datasetName": dataset_name, "fields": list(fields), "disaggregated": disaggregated},
    }


def frame(title: str) -> dict[str, Any]:
    return {"showTitle": True, "title": title}


def place(widget: dict[str, Any], x: int, y: int, width: int, height: int) -> dict[str, Any]:
    return {"widget": widget, "position": {"x": x, "y": y, "width": width, "height": height}}


def text(name: str, *lines: str) -> dict[str, Any]:
    return {"name": name, "multilineTextboxSpec": {"lines": list(lines)}}


def counter(name: str, dataset_name: str, column: str, title: str) -> dict[str, Any]:
    value = f"sum({column})"
    return {
        "name": name,
        "queries": [query(dataset_name, [field(value, f"SUM(`{column}`)")], disaggregated=False)],
        "spec": {
            "version": 2,
            "widgetType": "counter",
            "encodings": {"value": {"fieldName": value, "displayName": title}},
            "frame": frame(title),
        },
    }


def multi_select_filter(name: str, column: str, title: str, dataset_names: Sequence[str]) -> dict[str, Any]:
    """A filter on a column, applied to each dataset of the page having it."""
    queries = [
        query(dataset_name, [field(column)], disaggregated=False, name=f"{dataset_name}_{column}")
        for dataset_name in dataset_names
    ]
    return {
        "name": name,
        "queries": queries,
        "spec": {
            "version": 2,
            "widgetType": "filter-multi-select",
            "encodings": {
                "fields": [{"fieldName": column, "displayName": title, "queryName": item["name"]} for item in queries]
            },
            "frame": frame(title),
        },
    }


def table_widget(name: str, dataset_name: str, columns: Sequence[tuple[str, str, str]], title: str) -> dict[str, Any]:
    """A table of the rows of a dataset; each column is (name, title, type)."""
    display = {"string": "string", "integer": "number", "datetime": "datetime"}
    encodings = []
    for order, (column, column_title, column_type) in enumerate(columns):
        encoding: dict[str, Any] = {
            "fieldName": column,
            "type": column_type,
            "displayAs": display[column_type],
            "visible": True,
            "order": 100000 + order,
            "title": column_title,
            "displayName": column_title,
            "alignContent": "right" if column_type == "integer" else "left",
            "allowSearch": column_type == "string",
            "allowHTML": False,
            "highlightLinks": False,
            "useMonospaceFont": column == "cle",
            "preserveWhitespace": False,
        }
        if column_type == "integer":
            encoding["numberFormat"] = "0"
        encodings.append(encoding)
    return {
        "name": name,
        "queries": [query(dataset_name, [field(column) for column, _, _ in columns], disaggregated=True)],
        "spec": {
            "version": 1,
            "widgetType": "table",
            "encodings": {"columns": encodings},
            "invisibleColumns": [],
            "allowHTMLByDefault": False,
            "itemsPerPage": 25,
            "paginationSize": "default",
            "condensed": True,
            "withRowNumber": False,
            "frame": frame(title),
        },
    }


def occurrences_chart(name: str, dataset_name: str) -> dict[str, Any]:
    """Horizontal bars: number of occurrences of each check, by level."""
    count = "sum(occurrences)"
    return {
        "name": name,
        "queries": [
            query(
                dataset_name,
                [field("validation"), field("niveau"), field(count, "SUM(`occurrences`)")],
                disaggregated=False,
            )
        ],
        "spec": {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {"fieldName": count, "scale": {"type": "quantitative"}, "displayName": "Occurrences"},
                "y": {
                    "fieldName": "validation",
                    "scale": {"type": "categorical", "sort": {"by": "x-reversed"}},
                    "displayName": "Validation",
                },
                "color": {"fieldName": "niveau", "scale": {"type": "categorical"}, "displayName": "Niveau"},
            },
            "frame": frame("Occurrences par validation"),
        },
    }


def build(tables: Sequence[Table]) -> dict[str, Any]:
    """Returns the dashboard of the quality of the tables."""
    status, by_check, occurrences = "statut_lignes", "occurrences_par_validation", "occurrences"
    statistics = [
        place(
            text(
                "titre_statistiques",
                "## Qualité des données du Registraire des entreprises",
                "Résultat des validations DQX de la dernière exécution. Une ligne qui a des erreurs et des "
                "avertissements est comptée en erreur.",
            ),
            0,
            0,
            6,
            2,
        ),
        place(multi_select_filter("filtre_table_statistiques", "nom_table", "Table", [status, by_check]), 0, 2, 2, 2),
        place(counter("lignes_ok", status, "lignes_ok", "Lignes OK"), 0, 4, 2, 3),
        place(counter("lignes_avertissement", status, "lignes_avertissement", "Lignes en avertissement"), 2, 4, 2, 3),
        place(counter("lignes_erreur", status, "lignes_erreur", "Lignes en erreur"), 4, 4, 2, 3),
        place(
            table_widget(
                "statut_par_table",
                status,
                [
                    ("nom_table", "Table", "string"),
                    ("lignes", "Lignes", "integer"),
                    ("lignes_ok", "OK", "integer"),
                    ("lignes_avertissement", "En avertissement", "integer"),
                    ("lignes_erreur", "En erreur", "integer"),
                ],
                "Lignes par table",
            ),
            0,
            7,
            6,
            5,
        ),
        place(occurrences_chart("occurrences_par_validation", by_check), 0, 12, 6, 9),
    ]
    details = [
        place(
            text(
                "titre_occurrences",
                "## Occurrences des validations",
                "Chaque erreur et chaque avertissement, avec la clé primaire de la ligne concernée "
                "(toutes ses colonnes pour une table sans clé primaire).",
            ),
            0,
            0,
            6,
            2,
        ),
        place(multi_select_filter("filtre_table_occurrences", "nom_table", "Table", [occurrences]), 0, 2, 2, 2),
        place(multi_select_filter("filtre_validation", "validation", "Validation", [occurrences]), 2, 2, 2, 2),
        place(multi_select_filter("filtre_niveau", "niveau", "Niveau", [occurrences]), 4, 2, 2, 2),
        place(
            table_widget(
                "liste_occurrences",
                occurrences,
                [
                    ("nom_table", "Table", "string"),
                    ("cle", "Clé", "string"),
                    ("niveau", "Niveau", "string"),
                    ("validation", "Validation", "string"),
                    ("message", "Message", "string"),
                    ("colonnes", "Colonnes", "string"),
                    ("fonction", "Fonction DQX", "string"),
                    ("date_verification", "Date de vérification", "datetime"),
                ],
                "Occurrences",
            ),
            0,
            4,
            6,
            16,
        ),
    ]
    return {
        "datasets": [
            dataset(status, "Statut des lignes par table", row_status_query(tables)),
            dataset(by_check, "Occurrences par validation", occurrences_by_check_query(tables)),
            dataset(occurrences, "Occurrences", occurrences_query(tables)),
        ],
        "pages": [
            {
                "name": "statistiques",
                "displayName": "Statistiques",
                "pageType": "PAGE_TYPE_CANVAS",
                "layout": statistics,
            },
            {"name": "occurrences", "displayName": "Occurrences", "pageType": "PAGE_TYPE_CANVAS", "layout": details},
        ],
    }


def render(data_context: DataContext | None = None) -> str:
    """Returns the serialized dashboard, as committed."""
    data_context = data_context or DataContext.discover(PRODUCTS)
    return json.dumps(build(checked_tables(data_context)), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    DASHBOARD.write_text(render(), encoding="utf-8", newline="\n")
    print(f"Wrote {DASHBOARD}")
