"""Quality dashboards of the data products, generated from their contracts.

The dashboard of a data product is a Databricks AI/BI (Lakeview) dashboard showing the
results of the quality service: the DQX errors and warnings written in the rows of its
checked tables. Everything comes from the contracts and the settings:

- the tables checked by the quality service (see ``cds.services.checks.checked_tables``),
  named in the environment of the service context;
- the primary key of each table, which identifies its rows, without the classified
  columns the dashboard may not show (``DashboardSettings.shown_classifications``); a
  table without primary key identifies its rows by all its columns;
- the result columns of the quality service (``QualitySettings``).

It has two pages: **Statistics** (the status of the rows, the occurrences of each check)
and **Occurrences** (each error and warning, with the key of its row). ``build_dashboard``
returns its title and serialized configuration.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from cds.config import DashboardSettings, QualitySettings, resolved
from cds.context import ServiceContext
from cds.data_context import DataProduct, Table
from cds.errors import ConfigurationError
from cds.services.checks import checked_tables
from cds.sql import quote_identifier, quote_name, string_literal

# Status of a row: a row with errors and warnings is in error.
ERROR = "Error"
WARNING = "Warning"
OK = "OK"
STATUS_COLORS = {OK: "#00A972", WARNING: "#FFAB00", ERROR: "#FF3621"}
SEVERITY_COLORS = {WARNING: "#FFAB00", ERROR: "#FF3621"}
# Format of the check times, in the time zone of the warehouse (UTC by default).
TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"


def title(data_product: DataProduct) -> str:
    """The title of the dashboard: that of the settings, else ``<name> data quality``."""
    configured = data_product.settings(DashboardSettings).title
    if configured:
        return configured
    document = data_product.document
    name = document.name if document is not None and document.name else data_product.qualified_name
    return f"{name} data quality"


def key_expression(table: Table, shown: Sequence[str]) -> str:
    """SQL expression of the key of a row, as a JSON object: column name to value.

    The key is the primary key of the contract; rows of a table without primary key are
    identified by all their columns. The columns of the other classifications than
    ``shown`` are left out: the dashboard runs with the credentials of its publisher, which
    column masks may exempt.

    Raises:
        ConfigurationError: If every column of the key is left out.
    """
    hidden = {prop.column for prop in table.columns if prop.classification and prop.classification not in shown}
    columns = [
        column for column in table.primary_key or [prop.column for prop in table.columns] if column not in hidden
    ]
    if not columns:
        raise ConfigurationError(
            f"{table.qualified_name}: every column of its key is classified, its rows cannot be shown."
        )
    pairs = ", ".join(f"{string_literal(column)}, {quote_identifier(column)}" for column in columns)
    # Null values are kept: the key shows all its columns.
    return f"to_json(named_struct({pairs}), map('ignoreNullFields', 'false'))"


def rows_query(tables: Sequence[Table], context: ServiceContext, shown: Sequence[str]) -> str:
    """Query of the rows of all the tables: their table, key, errors and warnings."""
    selects = []
    for table in tables:
        settings = table.settings(QualitySettings)
        selects.append(
            f"  SELECT {string_literal(table.name)} AS table_name, {key_expression(table, shown)} AS row_key,\n"
            f"    {quote_identifier(resolved(settings.errors_column))} AS errors,\n"
            f"    {quote_identifier(resolved(settings.warnings_column))} AS warnings\n"
            f"  FROM {quote_name(context.full_table_name(table))}"
        )
    return "WITH checked_rows AS (\n" + "\n  UNION ALL\n".join(selects) + "\n)"


def row_status_query(rows: str) -> str:
    """Number of rows of each table by status: without issue, with warnings only, with errors."""
    return (
        f"{rows}\n"
        "SELECT table_name,\n"
        f"  CASE WHEN coalesce(size(errors), 0) > 0 THEN '{ERROR}'\n"
        f"    WHEN coalesce(size(warnings), 0) > 0 THEN '{WARNING}'\n"
        f"    ELSE '{OK}' END AS status,\n"
        "  count(*) AS row_count\n"
        "FROM checked_rows\n"
        "GROUP BY ALL"
    )


def _occurrences(rows: str) -> str:
    return (
        f"{rows},\n"
        "occurrences AS (\n"
        f"  SELECT table_name, row_key, '{ERROR}' AS severity, result\n"
        "  FROM checked_rows LATERAL VIEW explode(errors) e AS result\n"
        "  UNION ALL\n"
        f"  SELECT table_name, row_key, '{WARNING}' AS severity, result\n"
        "  FROM checked_rows LATERAL VIEW explode(warnings) w AS result\n"
        ")\n"
    )


def occurrences_query(rows: str) -> str:
    """Each error and warning of each row, with the key of the row."""
    return (
        f"{_occurrences(rows)}"
        "SELECT table_name, row_key, severity,\n"
        "  result.name AS check_name,\n"
        "  result.function AS dqx_function,\n"
        "  array_join(result.columns, ', ') AS checked_columns,\n"
        "  result.message AS message,\n"
        # Formatted as text: the table widget showed the timestamp as epoch milliseconds.
        f"  date_format(result.run_time, '{TIME_FORMAT}') AS checked_at\n"
        "FROM occurrences"
    )


def occurrences_by_check_query(rows: str) -> str:
    """Number of errors and warnings of each check, by table."""
    return (
        f"{_occurrences(rows)}"
        "SELECT table_name, result.name AS check_name, severity, count(*) AS occurrences\n"
        "FROM occurrences\n"
        "GROUP BY table_name, result.name, severity"
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


def colors(scale: Mapping[str, str]) -> dict[str, Any]:
    """A categorical scale giving a color to each value."""
    return {"type": "categorical", "mappings": [{"value": value, "color": color} for value, color in scale.items()]}


def pie_chart(name: str, dataset_name: str) -> dict[str, Any]:
    """Share of the rows of each status."""
    count = "sum(row_count)"
    return {
        "name": name,
        "queries": [query(dataset_name, [field("status"), field(count, "SUM(`row_count`)")], disaggregated=False)],
        "spec": {
            "version": 3,
            "widgetType": "pie",
            "encodings": {
                "angle": {"fieldName": count, "scale": {"type": "quantitative"}, "displayName": "Records"},
                "color": {"fieldName": "status", "scale": colors(STATUS_COLORS), "displayName": "Status"},
            },
            "frame": frame("Status"),
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
    display = {"string": "string", "integer": "number"}
    encodings = []
    for order, (column, column_title, column_type) in enumerate(columns):
        # The keys of a column, as the dashboard editor writes them.
        encoding: dict[str, Any] = {
            "fieldName": column,
            "booleanValues": ["false", "true"],
            "imageUrlTemplate": "{{ @ }}",
            "imageTitleTemplate": "{{ @ }}",
            "imageWidth": "",
            "imageHeight": "",
            "linkUrlTemplate": "{{ @ }}",
            "linkTextTemplate": "{{ @ }}",
            "linkTitleTemplate": "{{ @ }}",
            "linkOpenInNewTab": True,
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
            "useMonospaceFont": column == "row_key",
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
    """Horizontal bars: number of occurrences of each check, stacked by severity."""
    count = "sum(occurrences)"
    return {
        "name": name,
        "queries": [
            query(
                dataset_name,
                [field("check_name"), field("severity"), field(count, "SUM(`occurrences`)")],
                disaggregated=False,
            )
        ],
        "spec": {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {"fieldName": count, "scale": {"type": "quantitative"}, "displayName": "Occurrences"},
                "y": {
                    "fieldName": "check_name",
                    "scale": {"type": "categorical", "sort": {"by": "x-reversed"}},
                    "displayName": "Check",
                },
                "color": {"fieldName": "severity", "scale": colors(SEVERITY_COLORS), "displayName": "Severity"},
            },
            "frame": frame("Occurrences by check"),
        },
    }


def serialized_dashboard(tables: Sequence[Table], context: ServiceContext, shown: Sequence[str]) -> dict[str, Any]:
    """The configuration of the dashboard of the quality of tables."""
    rows = rows_query(tables, context, shown)
    status, by_check, occurrences = "row_status", "occurrences_by_check", "occurrences"
    statistics = [
        place(multi_select_filter("statistics_table_filter", "table_name", "Table", [status, by_check]), 0, 0, 2, 1),
        place(pie_chart("row_status_chart", status), 0, 1, 3, 8),
        place(occurrences_chart("occurrences_by_check_chart", by_check), 3, 1, 3, 8),
        place(
            table_widget(
                "occurrences_by_table",
                by_check,
                [
                    ("table_name", "Table", "string"),
                    ("check_name", "Check", "string"),
                    ("severity", "Severity", "string"),
                    ("occurrences", "Count", "integer"),
                ],
                "Occurrences by table and check",
            ),
            0,
            9,
            6,
            8,
        ),
    ]
    details = [
        place(multi_select_filter("occurrences_table_filter", "table_name", "Table", [occurrences]), 0, 0, 2, 1),
        place(multi_select_filter("check_filter", "check_name", "Check", [occurrences]), 2, 0, 2, 1),
        place(multi_select_filter("severity_filter", "severity", "Severity", [occurrences]), 4, 0, 2, 1),
        place(
            table_widget(
                "occurrence_list",
                occurrences,
                [
                    ("table_name", "Table", "string"),
                    ("row_key", "Key", "string"),
                    ("severity", "Severity", "string"),
                    ("check_name", "Check", "string"),
                    ("message", "Message", "string"),
                    ("checked_columns", "Columns", "string"),
                    ("dqx_function", "DQX function", "string"),
                    ("checked_at", "Checked at (UTC)", "string"),
                ],
                "Occurrences",
            ),
            0,
            1,
            6,
            16,
        ),
    ]
    return {
        "datasets": [
            dataset(status, "Row status by table", row_status_query(rows)),
            dataset(by_check, "Occurrences by check", occurrences_by_check_query(rows)),
            dataset(occurrences, "Occurrences", occurrences_query(rows)),
        ],
        "pages": [
            {
                "name": "statistics",
                "displayName": "Statistics",
                "pageType": "PAGE_TYPE_CANVAS",
                "layout": statistics,
            },
            {"name": "occurrences", "displayName": "Occurrences", "pageType": "PAGE_TYPE_CANVAS", "layout": details},
        ],
    }


def build_dashboard(data_product: DataProduct, context: ServiceContext) -> dict[str, Any]:
    """The quality dashboard of a data product: its ``title`` and ``serialized_dashboard``.

    The ``serialized_dashboard`` is a mapping: the Lakeview API takes it serialized as a
    JSON string.

    Raises:
        ConfigurationError: If no table of the data product is checked by the quality service.
    """
    tables = checked_tables(data_product, context)
    if not tables:
        raise ConfigurationError(f"{data_product.qualified_name}: no table is checked by the quality service.")
    shown = resolved(data_product.settings(DashboardSettings).shown_classifications)
    return {"title": title(data_product), "serialized_dashboard": serialized_dashboard(tables, context, shown)}
