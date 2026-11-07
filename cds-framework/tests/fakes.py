"""Test doubles of the Spark objects used by the services: no Spark is needed."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class Row(dict[str, Any]):
    """A Spark row: a mapping whose values are also attributes."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


class FakeType:
    def __init__(self, simple: str, json_value: str | dict[str, Any] | None = None) -> None:
        self._simple = simple
        self._json = json_value if json_value is not None else simple

    def simpleString(self) -> str:
        return self._simple

    def jsonValue(self) -> str | dict[str, Any]:
        return self._json


class FakeField:
    def __init__(self, name: str, data_type: str | FakeType = "string", comment: str | None = None) -> None:
        self.name = name
        self.dataType = data_type if isinstance(data_type, FakeType) else FakeType(data_type)
        self.metadata = {"comment": comment} if comment is not None else {}


class FakeSchema:
    def __init__(self, fields: Sequence[FakeField]) -> None:
        self.fields = list(fields)

    def __getitem__(self, name: str) -> FakeField:
        return next(field for field in self.fields if field.name == name)


class FakeDataFrame:
    """A DataFrame recording the transformations applied to it."""

    def __init__(
        self,
        fields: Iterable[FakeField | str] = (),
        rows: Sequence[Row] = (),
        spark: FakeSpark | None = None,
        operations: Sequence[tuple[str, Any]] = (),
    ) -> None:
        self._fields = [field if isinstance(field, FakeField) else FakeField(field) for field in fields]
        self._rows = list(rows)
        self.spark = spark
        self.operations = list(operations)

    def as_dataframe(self) -> DataFrame:
        """Returns this fake typed as a DataFrame, to pass it to the framework."""
        return cast("DataFrame", self)

    @property
    def columns(self) -> list[str]:
        return [field.name for field in self._fields]

    @property
    def schema(self) -> FakeSchema:
        return FakeSchema(self._fields)

    def collect(self) -> list[Row]:
        return list(self._rows)

    def selectExpr(self, *expressions: str) -> FakeDataFrame:
        return self._derive([alias(expression) for expression in expressions], ("selectExpr", expressions))

    def drop(self, *columns: str) -> FakeDataFrame:
        return self._derive([field for field in self._fields if field.name not in columns], ("drop", columns))

    def with_fields(self, *fields: FakeField | str) -> FakeDataFrame:
        """Returns a copy with additional fields, as DQX adds its result columns."""
        return self._derive(
            self._fields + [f if isinstance(f, FakeField) else FakeField(f) for f in fields], ("with", fields)
        )

    def createOrReplaceTempView(self, name: str) -> None:
        assert self.spark is not None, "the DataFrame has no session"
        self.spark.views[name] = self

    def _derive(self, fields: Iterable[FakeField | str], operation: tuple[str, Any]) -> FakeDataFrame:
        return FakeDataFrame(fields, self._rows, self.spark, [*self.operations, operation])


Responder = Callable[[str], Sequence[Row]]
Response = Sequence[Row] | Responder | Exception


class FakeSpark:
    """A Spark session recording the SQL statements it runs.

    ``tables`` maps unquoted full names to DataFrames, read by ``table``. ``on`` registers
    the result of the statements matching a regular expression.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.tables: dict[str, FakeDataFrame] = {}
        self.views: dict[str, FakeDataFrame] = {}
        self.catalog = MagicMock()
        self._responses: list[tuple[re.Pattern[str], Response]] = []

    def as_session(self) -> SparkSession:
        """Returns this fake typed as a Spark session, to pass it to the framework."""
        return cast("SparkSession", self)

    def on(self, pattern: str, result: Response) -> None:
        self._responses.insert(0, (re.compile(pattern, re.DOTALL), result))

    def add_table(self, full_name: str, fields: Iterable[FakeField | str]) -> FakeDataFrame:
        df = FakeDataFrame(fields, spark=self)
        self.tables[full_name] = df
        schema, _, table = full_name.rpartition(".")
        existing = [row for row in self._show_tables(schema)]
        self.on(rf"^SHOW TABLES IN {re.escape(_quote(schema))}$", [*existing, Row(tableName=table, isTemporary=False)])
        return df

    def _show_tables(self, schema: str) -> list[Row]:
        statement = f"SHOW TABLES IN {_quote(schema)}"
        for pattern, result in self._responses:
            if pattern.search(statement) and isinstance(result, list):
                return result
        return []

    def table(self, name: str) -> FakeDataFrame:
        return self.tables[name.replace("`", "")]

    def sql(self, statement: str) -> FakeDataFrame:
        self.statements.append(statement)
        for pattern, result in self._responses:
            if pattern.search(statement):
                if isinstance(result, Exception):
                    raise result
                rows = result(statement) if callable(result) else result
                return FakeDataFrame(rows=rows, spark=self)
        return FakeDataFrame(spark=self)

    def statements_matching(self, pattern: str) -> list[str]:
        return [statement for statement in self.statements if re.search(pattern, statement, re.DOTALL)]


def alias(expression: str) -> str:
    """Returns the alias of a ``selectExpr`` expression ending with ``AS `name```."""
    match = re.search(r"AS `([^`]+)`\s*$", expression)
    assert match is not None, f"no alias in {expression!r}"
    return match.group(1)


def _quote(name: str) -> str:
    return ".".join(f"`{part}`" for part in name.split("."))
