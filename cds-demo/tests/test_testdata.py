"""Tests of the synthetic data set (cds-demo/csv/testdata), without Spark.

The checks of the contracts are built as the quality service builds them, then applied
to the committed files by a small evaluator of the DQX functions they use. Each row must
fail exactly the checks the generator expects, and the files must keep the distribution
of the data set: 85 % of valid rows, 10 % of rows with warnings and 5 % with errors.
"""

from __future__ import annotations

import csv
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, cast

import generate_testdata
import pytest
from pyspark.sql import SparkSession

from cds import DataContext, RuntimeConfiguration, ServiceContext, Table
from cds.services.checks import build_checks

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
# The contracts document the source file of each table and the source column of each property.
SOURCE_FILE = re.compile(r"fichier source (\w+\.csv)")
SOURCE_COLUMN = re.compile(r"Colonne source (\w+)\.")

Value = str | int | bool | None
Row = dict[str, Value]


def ge(left: Value, right: Value) -> bool | None:
    return None if left is None or right is None else cast("str", left) >= cast("str", right)


def ne(left: Value, right: Value) -> bool | None:
    return None if left is None or right is None else left != right


def sql_or(*values: bool | None) -> bool | None:
    if any(value is True for value in values):
        return True
    return None if any(value is None for value in values) else False


# The SQL expressions of the contracts, in Python. None is the null of SQL: a row whose
# expression is null passes, as with DQX.
EXPRESSIONS: Mapping[str, Callable[[Row], bool | None]] = {
    "date_fin_responsabilite_limitee >= date_debut_responsabilite_limitee": lambda row: ge(
        row["date_fin_responsabilite_limitee"], row["date_debut_responsabilite_limitee"]
    ),
    "date_retrait >= date_inscription": lambda row: ge(row["date_retrait"], row["date_inscription"]),
    "code_statut_nom <> 'A' OR date_retrait IS NOT NULL": lambda row: sql_or(
        ne(row["code_statut_nom"], "A"), row["date_retrait"] is not None
    ),
    "numero_entreprise_liee <> numero_entreprise": lambda row: ne(
        row["numero_entreprise_liee"], row["numero_entreprise"]
    ),
    "date_effet <= current_date()": lambda row: ge(date.today().isoformat(), row["date_effet"]),
    # trim removes the spaces only.
    "code = trim(code)": lambda row: (
        None if row["code"] is None else row["code"] == cast("str", row["code"]).strip(" ")
    ),
}


def literal(value: object) -> object:
    """Returns the value of a SQL string literal of an ``allowed`` list; other values are unchanged."""
    if isinstance(value, str) and len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("\\'", "'").replace("\\\\", "\\")
    return value


def source_file(table: Table) -> str:
    match = SOURCE_FILE.search(table.description or "")
    assert match, f"{table.name}: the description does not name the source file"
    return match.group(1)


def contract_rows(table: Table, path: Path) -> list[Row]:
    """Reads a file of the data set as the transformation does: renamed and cast to the contract types."""
    with path.open(encoding="utf-8-sig", newline="") as file:
        # Spark reads the empty values of a CSV file as nulls.
        source = [{key: value or None for key, value in row.items()} for row in csv.DictReader(file)]
    rows: list[Row] = [{} for _ in source]
    for prop in table.columns:
        match = SOURCE_COLUMN.search(prop.description or "")
        assert match, f"{table.name}.{prop.column}: the description does not name the source column"
        data_type = prop.data_type.upper()
        for row, values in zip(rows, source, strict=True):
            value = values[match.group(1)]
            if data_type == "BOOLEAN":
                # sources.indicator: O and N become booleans, other values nulls.
                indicator = (value or "").strip().upper()
                row[prop.column] = {"O": True, "N": False}.get(indicator)
            elif data_type == "INT" and value is not None:
                row[prop.column] = int(value)
            else:
                row[prop.column] = value
    return rows


def failures(check: Mapping[str, Any], rows: Sequence[Row], tables: Mapping[str, list[Row]]) -> list[bool]:
    """Returns whether each row fails a DQX check."""
    function = check["check"]["function"]
    arguments = check["check"]["arguments"]
    if function == "is_not_null":
        return [row[arguments["column"]] is None for row in rows]
    if function == "is_in_list":
        allowed = {literal(value) for value in arguments["allowed"]}
        return [row[arguments["column"]] is not None and row[arguments["column"]] not in allowed for row in rows]
    if function == "regex_match":
        values = [row[arguments["column"]] for row in rows]
        return [value is not None and not re.search(arguments["regex"], str(value)) for value in values]
    if function == "is_not_less_than":
        values = [row[arguments["column"]] for row in rows]
        return [value is not None and cast("int", value) < arguments["limit"] for value in values]
    if function == "is_unique":
        keys = [tuple(row[column] for column in arguments["columns"]) for row in rows]
        # By default, DQX ignores the keys with nulls.
        compared = [not arguments.get("nulls_distinct", True) or None not in key for key in keys]
        counts = Counter(key for key, used in zip(keys, compared, strict=True) if used)
        return [used and counts[key] > 1 for key, used in zip(keys, compared, strict=True)]
    if function == "foreign_key":
        referenced = tables[arguments["ref_table"].replace("`", "").rsplit(".", 1)[1]]
        known = {tuple(row[column] for column in arguments["ref_columns"]) for row in referenced}
        keys = [tuple(row[column] for column in arguments["columns"]) for row in rows]
        return [None not in key and key not in known for key in keys]
    if function == "is_aggr_not_less_than":
        assert (arguments["column"], arguments["aggr_type"]) == ("*", "count")
        return [len(rows) < arguments["limit"]] * len(rows)
    if function == "sql_expression":
        expression = arguments["expression"]
        assert expression in EXPRESSIONS, f"the evaluator does not implement the expression {expression!r}"
        return [EXPRESSIONS[expression](row) is False for row in rows]
    raise AssertionError(f"the evaluator does not implement the DQX function {function}")


@dataclass
class Result:
    """The checks failed by a row."""

    errors: set[str] = field(default_factory=set)
    warnings: set[str] = field(default_factory=set)


# The query of the allowed values of a column (allowedValues custom property of the contracts).
ALLOWED_VALUES_QUERY = re.compile(
    r"SELECT DISTINCT `(?P<column>\w+)` AS value FROM `\w+`\.`\w+`\.`(?P<table>\w+)` "
    r"WHERE `(?P=column)` IS NOT NULL AND \((?P<filtered>\w+) = '(?P<value>\w+)'\)"
)


@dataclass
class Values:
    """The result of a query: its rows."""

    rows: list[Row]

    def collect(self) -> list[Row]:
        return self.rows


class Spark:
    """Answers the queries of allowed values from the rows of the committed files."""

    def __init__(self, tables: Mapping[str, list[Row]]) -> None:
        self.tables = tables

    def sql(self, statement: str) -> Values:
        match = ALLOWED_VALUES_QUERY.fullmatch(statement)
        assert match, f"unexpected query: {statement}"
        column, filtered, value = match["column"], match["filtered"], match["value"]
        found = {row[column] for row in self.tables[match["table"]] if row[filtered] == value}
        return Values([{"value": code} for code in found if code is not None])


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


@pytest.fixture(scope="module")
def tables(data_context: DataContext) -> dict[str, list[Row]]:
    return {
        table.name: contract_rows(table, generate_testdata.TESTDATA / synthetic_file_name(source_file(table)))
        for table in data_context.tables
    }


@pytest.fixture(scope="module")
def results(data_context: DataContext, tables: dict[str, list[Row]]) -> dict[str, list[Result]]:
    context = ServiceContext(
        spark=cast("SparkSession", Spark(tables)),
        environment="nprod",
        data_context=data_context,
        runtime_configuration=RuntimeConfiguration({"environment": "nprod", "synthetic_data": "true"}),
    )
    found = {}
    for table in data_context.tables:
        rows = tables[table.name]
        found[table.name] = [Result() for _ in rows]
        for check in build_checks(table, context).checks:
            for result, failed in zip(found[table.name], failures(check, rows, tables), strict=True):
                if failed:
                    (result.errors if check["criticality"] == "error" else result.warnings).add(check["name"])
    return found


def synthetic_file_name(file_name: str) -> str:
    stem, extension = file_name.rsplit(".", 1)
    return f"{stem}{generate_testdata.SUFFIX}.{extension}"


TABLES = [
    "continuation_transformation",
    "domaine_valeur",
    "entreprise",
    "etablissement",
    "fusion_scission",
    "nom_entreprise",
]


def test_the_committed_files_are_up_to_date() -> None:
    expected = generate_testdata.render()
    committed = {path.name: path.read_text(encoding="utf-8") for path in generate_testdata.TESTDATA.glob("*.csv")}

    assert committed.keys() == expected.keys()
    for name, content in expected.items():
        assert committed[name] == content, (
            f"{name} does not match the generator: run uv run python cds-demo/csv/generate_testdata.py"
        )


@pytest.mark.parametrize("table_name", TABLES)
def test_each_row_fails_the_expected_checks(
    data_context: DataContext, results: dict[str, list[Result]], table_name: str
) -> None:
    [source] = [
        source
        for source in generate_testdata.generate()
        if source.name == source_file(data_context.get_table(table_name))
    ]
    found = results[table_name]

    assert len(found) == len(source.expected)
    unexpected = {
        index: (sorted(result.errors | result.warnings), sorted(expected))
        for index, (result, expected) in enumerate(zip(found, source.expected, strict=True))
        if result.errors | result.warnings != expected
    }
    assert not unexpected, f"rows failing other checks than expected (found, expected): {unexpected}"
    assert {index for index, result in enumerate(found) if result.errors} == source.errors


@pytest.mark.parametrize("table_name", TABLES)
def test_the_distribution_of_the_rows(results: dict[str, list[Result]], table_name: str) -> None:
    found = results[table_name]
    errors = sum(1 for result in found if result.errors)
    warnings = sum(1 for result in found if result.warnings and not result.errors)

    assert (len(found), errors, warnings) == (200, 10, 20)


def test_the_rows_with_errors_illustrate_several_checks(results: dict[str, list[Result]]) -> None:
    for table_name, found in results.items():
        checks = set().union(*(result.errors for result in found))
        assert len(checks) >= 4, f"{table_name}: {sorted(checks)}"


def test_the_references_are_kept(tables: dict[str, list[Row]]) -> None:
    """The rows of the other tables reference enterprises without errors, unless they illustrate a foreign key."""
    neqs = Counter(row["numero_entreprise"] for row in tables["entreprise"])
    pattern = re.compile(r"^[0-9]{10}$")
    unknown = set(generate_testdata.UNKNOWN_NEQS)
    for table_name in ("etablissement", "nom_entreprise", "fusion_scission", "continuation_transformation"):
        for row in tables[table_name]:
            for column in ("numero_entreprise", "numero_entreprise_liee"):
                neq = row.get(column)
                if neq is None or neq in unknown:
                    continue
                assert neqs[neq] == 1 and pattern.match(cast("str", neq)), f"{table_name}.{column}: {neq}"
