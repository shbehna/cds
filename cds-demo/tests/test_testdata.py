"""Tests of the synthetic data set (cds-demo/csv/testdata), without Spark.

The checks of the contracts are built as the quality service builds them, then applied
to the committed files by a small evaluator of the DQX functions they use. Each row must
fail exactly the checks the generator expects, and the files must keep the distribution
of the data set: 85 % of valid rows, 10 % of rows with warnings and 5 % with errors.

The tests read the tables, the source columns and the checks from the contracts: changing
a contract may need regenerating the data set, not changing the tests.
"""

from __future__ import annotations

import csv
import operator
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import generate_testdata
import pytest
from pyspark.sql import SparkSession

from cds import DataContext, RuntimeConfiguration, ServiceContext, Table
from cds.services.checks import build_checks

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
TABLES = sorted(table.name for table in DataContext.discover(PRODUCTS).tables)
# The contracts document the source file of each table and the source column of each property.
SOURCE_FILE = re.compile(r"fichier source (\w+\.csv)")
SOURCE_COLUMN = re.compile(r"Colonne source (\w+)\.")

Value = str | int | float | bool | None
Row = dict[str, Value]
Check = dict[str, Any]
# Spark types read as Python numbers; the other columns are strings, or booleans.
INTEGER_TYPES = ("INT", "INTEGER", "BIGINT", "LONG", "SMALLINT", "SHORT", "TINYINT", "BYTE")
DECIMAL_TYPES = ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC")


def is_number(value: Value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


class Expression:
    """A SQL expression of the contracts (sql_expression checks), evaluated on a row.

    The evaluator knows the subset of SQL the contracts use: comparisons of columns,
    literals (strings, numbers, TRUE, FALSE, NULL) and function calls (trim, upper,
    lower, current_date), IS [NOT] NULL, AND, OR, NOT and parentheses. The dates are ISO
    strings, compared as strings. A string compared with a number is cast to a number, as
    Spark does: null when it is not one. None is the null of SQL, with its three-valued logic.
    """

    TOKEN = re.compile(r"\s*(<>|!=|<=|>=|=|<|>|\(|\)|,|'(?:[^']|'')*'|\d+(?:\.\d+)?|\w+)")
    COMPARISONS: Mapping[str, Callable[[Any, Any], bool]] = {
        "=": operator.eq,
        "<>": operator.ne,
        "!=": operator.ne,
        "<": operator.lt,
        "<=": operator.le,
        ">": operator.gt,
        ">=": operator.ge,
    }

    def __init__(self, text: str) -> None:
        self.text = text
        self.tokens: list[str] = []
        position = 0
        while position < len(text.rstrip()):
            match = self.TOKEN.match(text, position)
            assert match, f"the evaluator cannot read {text!r} at {text[position:]!r}"
            self.tokens.append(match.group(1))
            position = match.end()

    def evaluate(self, row: Row) -> bool | None:
        self.row, self.index = row, 0
        result = self._or()
        assert self.index == len(self.tokens), f"the evaluator cannot read {self.text!r}"
        return cast("bool | None", result)

    def _peek(self) -> str:
        return self.tokens[self.index].upper() if self.index < len(self.tokens) else ""

    def _take(self, expected: str | None = None) -> str:
        token = self.tokens[self.index]
        assert expected is None or token.upper() == expected, f"{self.text!r}: expected {expected}, got {token}"
        self.index += 1
        return token

    def _or(self) -> Value:
        values = [self._and()]
        while self._peek() == "OR":
            self._take()
            values.append(self._and())
        if len(values) == 1:
            return values[0]
        if any(value is True for value in values):
            return True
        return None if any(value is None for value in values) else False

    def _and(self) -> Value:
        values = [self._not()]
        while self._peek() == "AND":
            self._take()
            values.append(self._not())
        if len(values) == 1:
            return values[0]
        if any(value is False for value in values):
            return False
        return None if any(value is None for value in values) else True

    def _not(self) -> Value:
        if self._peek() == "NOT":
            self._take()
            value = self._not()
            return None if value is None else not value
        return self._comparison()

    def _comparison(self) -> Value:
        left = self._operand()
        if self._peek() == "IS":
            self._take()
            negated = self._peek() == "NOT"
            if negated:
                self._take()
            self._take("NULL")
            return (left is not None) if negated else (left is None)
        if self._peek() not in self.COMPARISONS:
            return left
        symbol = self._take()
        left, right = self._coerce(left, self._operand())
        if left is None or right is None:
            return None
        return self.COMPARISONS[symbol](left, right)

    def _coerce(self, left: Value, right: Value) -> tuple[Value, Value]:
        """The operands of a comparison, of the same type: a string compared with a number becomes a number."""
        if left is None or right is None:
            return left, right
        if is_number(left) == is_number(right):
            assert is_number(left) or type(left) is type(right), (
                f"{self.text!r}: cannot compare {left!r} with {right!r}"
            )
            return left, right
        text = right if is_number(left) else left
        assert isinstance(text, str), f"{self.text!r}: cannot compare {left!r} with {right!r}"
        try:
            number = float(text)
        except ValueError:
            return None, None
        return (left, number) if is_number(left) else (number, right)

    def _operand(self) -> Value:
        token = self._take()
        if token == "(":
            value = self._or()
            self._take(")")
            return value
        if token.startswith("'"):
            return token[1:-1].replace("''", "'")
        if token[0].isdigit():
            return int(token) if token.isdigit() else float(token)
        if self._peek() == "(":
            return self._call(token.lower())
        if token.upper() in ("TRUE", "FALSE"):
            return token.upper() == "TRUE"
        if token.upper() == "NULL":
            return None
        assert token in self.row, f"{self.text!r}: unknown column {token}"
        return self.row[token]

    def _call(self, function: str) -> Value:
        self._take("(")
        arguments = [] if self._peek() == ")" else [self._or()]
        while self._peek() == ",":
            self._take()
            arguments.append(self._or())
        self._take(")")
        if function == "current_date" and not arguments:
            return date.today().isoformat()
        [value] = arguments
        if value is None:
            return None
        # trim removes the spaces only.
        functions: dict[str, Callable[[str], str]] = {
            "trim": lambda text: text.strip(" "),
            "upper": str.upper,
            "lower": str.lower,
        }
        assert function in functions, f"{self.text!r}: the evaluator does not implement {function}"
        return functions[function](str(value))


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
            assert match.group(1) in values, f"{table.name}.{prop.column}: no source column {match.group(1)}"
            value = values[match.group(1)]
            if data_type == "BOOLEAN":
                # sources.indicator: O and N become booleans, other values nulls.
                indicator = (value or "").strip().upper()
                row[prop.column] = {"O": True, "N": False}.get(indicator)
            elif data_type in INTEGER_TYPES and value is not None:
                row[prop.column] = int(value)
            elif data_type.startswith(DECIMAL_TYPES) and value is not None:
                row[prop.column] = float(value)
            else:
                row[prop.column] = value
    return rows


def referenced_table(check: Check) -> str:
    return cast("str", check["check"]["arguments"]["ref_table"]).replace("`", "").rsplit(".", 1)[1]


def failures(check: Check, rows: Sequence[Row], tables: Mapping[str, list[Row]]) -> list[bool]:
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
        referenced = tables[referenced_table(check)]
        known = {tuple(row[column] for column in arguments["ref_columns"]) for row in referenced}
        keys = [tuple(row[column] for column in arguments["columns"]) for row in rows]
        return [None not in key and key not in known for key in keys]
    if function == "is_aggr_not_less_than":
        assert (arguments["column"], arguments["aggr_type"]) == ("*", "count")
        return [len(rows) < arguments["limit"]] * len(rows)
    if function == "sql_expression":
        expression = Expression(arguments["expression"])
        return [expression.evaluate(row) is False for row in rows]
    raise AssertionError(f"the evaluator does not implement the DQX function {function}")


@dataclass
class Result:
    """The checks failed by a row."""

    errors: set[str] = field(default_factory=set)
    warnings: set[str] = field(default_factory=set)


# The query of the allowed values of a column (allowedValues custom property of the contracts).
ALLOWED_VALUES_QUERY = re.compile(
    r"SELECT DISTINCT `(?P<column>\w+)` AS value FROM `\w+`\.`\w+`\.`(?P<table>\w+)` "
    r"WHERE `(?P=column)` IS NOT NULL( AND \((?P<filter>.+)\))? LIMIT \d+"
)


@dataclass
class Values:
    """The result of a query: its rows."""

    rows: list[Row] | list[SimpleNamespace]

    def collect(self) -> list[Row] | list[SimpleNamespace]:
        return self.rows


class Spark:
    """Answers the queries of allowed values from the rows of the committed files."""

    def __init__(self, tables: Mapping[str, list[Row]]) -> None:
        self.tables = tables

    def sql(self, statement: str) -> Values:
        if statement.startswith("SHOW TABLES IN "):
            return Values([SimpleNamespace(tableName=name) for name in self.tables])
        match = ALLOWED_VALUES_QUERY.fullmatch(statement)
        assert match, f"unexpected query: {statement}"
        column, condition = match["column"], match["filter"]
        rows = self.tables[match["table"]]
        selected = [row for row in rows if Expression(condition).evaluate(row)] if condition else rows
        found = {row[column] for row in selected}
        values: list[Row] = [{"value": code} for code in found if code is not None]
        return Values(values)


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
def checks(data_context: DataContext, tables: dict[str, list[Row]]) -> dict[str, list[Check]]:
    context = ServiceContext(
        spark=cast("SparkSession", Spark(tables)),
        environment="nprod",
        data_context=data_context,
        runtime_configuration=RuntimeConfiguration({"environment": "nprod", "synthetic_data": "true"}),
    )
    return {table.name: build_checks(table, context).checks for table in data_context.tables}


@pytest.fixture(scope="module")
def results(tables: dict[str, list[Row]], checks: dict[str, list[Check]]) -> dict[str, list[Result]]:
    found = {}
    for table_name, table_checks in checks.items():
        rows = tables[table_name]
        found[table_name] = [Result() for _ in rows]
        for check in table_checks:
            for result, failed in zip(found[table_name], failures(check, rows, tables), strict=True):
                if failed:
                    (result.errors if check["criticality"] == "error" else result.warnings).add(check["name"])
    return found


def synthetic_file_name(file_name: str) -> str:
    stem, extension = file_name.rsplit(".", 1)
    return f"{stem}{generate_testdata.SUFFIX}.{extension}"


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

    # 5 % of rows with errors, 10 % with warnings only.
    rows = generate_testdata.ROWS
    assert (len(found), errors, warnings) == (rows, rows * 5 // 100, rows * 10 // 100)


def test_the_rows_with_errors_illustrate_several_checks(results: dict[str, list[Result]]) -> None:
    for table_name, found in results.items():
        failed = set().union(*(result.errors for result in found))
        assert len(failed) >= 4, f"{table_name}: {sorted(failed)}"


def test_the_references_are_kept(
    tables: dict[str, list[Row]], checks: dict[str, list[Check]], results: dict[str, list[Result]]
) -> None:
    """The rows reference a single row without errors, unless they illustrate a foreign key check."""
    for table_name, table_checks in checks.items():
        for check in (check for check in table_checks if check["check"]["function"] == "foreign_key"):
            arguments = check["check"]["arguments"]
            target = referenced_table(check)
            counts = Counter(tuple(row[column] for column in arguments["ref_columns"]) for row in tables[target])
            valid = {
                tuple(row[column] for column in arguments["ref_columns"])
                for row, result in zip(tables[target], results[target], strict=True)
                if not result.errors
            }
            for row, result in zip(tables[table_name], results[table_name], strict=True):
                key = tuple(row[column] for column in arguments["columns"])
                if None in key or check["name"] in result.errors | result.warnings:
                    continue
                assert counts[key] == 1 and key in valid, f"{table_name} ({check['name']}): {key}"
