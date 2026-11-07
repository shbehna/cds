"""Transformation service: produces the content of the tables from their sources."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from cds.data_context import DataProduct, Table
from cds.errors import ConfigurationError, ContractError
from cds.injection import ContractInjection
from cds.services.base import Service, TableResult
from cds.services.tables import last_operation_metrics, list_tables, overwrite
from cds.sql import quote_identifier

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def find_transformation(table: Table) -> Callable[..., Any] | None:
    """Returns the transformation configured for a table, if any.

    Raises:
        ConfigurationError: If the ``transform`` of the contract configuration is invalid.
    """
    transform: object = getattr(table.contract.config, "transform", None)
    if transform is None:
        return None
    if isinstance(transform, Mapping):
        function = transform.get(table.name)
        if function is not None and not callable(function):
            raise ConfigurationError(f"{table.contract.config_path}: the transform of {table.name} must be a function.")
        return function
    if not callable(transform):
        raise ConfigurationError(
            f"{table.contract.config_path}: transform must be a function or a mapping of table name to function."
        )
    if len(table.contract.tables) > 1:
        raise ConfigurationError(
            f"{table.contract.config_path}: the contract describes several tables, so transform "
            "must be a mapping of table name to function."
        )
    return transform


def conform_to_contract(df: DataFrame, table: Table) -> DataFrame:
    """Selects the columns of a contract in a DataFrame, cast to their contract types.

    Raises:
        ContractError: If the DataFrame misses columns of the contract, or has others.
    """
    available = {column.lower(): column for column in df.columns}
    expected = [prop.column.lower() for prop in table.columns]
    missing = [prop.column for prop in table.columns if prop.column.lower() not in available]
    unexpected = [column for key, column in available.items() if key not in expected]
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing columns: {', '.join(missing)}")
        if unexpected:
            details.append(f"columns not in the contract: {', '.join(unexpected)}")
        raise ContractError(
            f"The transformation of {table.qualified_name} does not match its contract ({'; '.join(details)})."
        )
    return df.selectExpr(
        *(
            f"CAST({quote_identifier(available[prop.column.lower()])} AS {prop.data_type}) "
            f"AS {quote_identifier(prop.column)}"
            for prop in table.columns
        )
    )


class TransformationService(Service):
    """Runs the transformation of each table and writes its result.

    The transformation is the ``transform`` of the contract configuration. It is called
    with injected parameters (see ``ContractInjection``), among which ``table``,
    ``contract`` and ``data_product``. Its DataFrame must have the columns of the
    contract, which are cast to their contract types; the content of the table is then
    replaced. Tables without a transformation are skipped.
    """

    name = "transformation"

    def prepare(self, data_product: DataProduct, tables: Sequence[Table]) -> None:
        schemas = {self.context.full_schema_name(table) for table in tables}
        self._existing = {name: list_tables(self.spark, name) for name in schemas}

    def process(self, table: Table) -> TableResult:
        transformation = find_transformation(table)
        if transformation is None:
            return TableResult.skipped(table.qualified_name, "no transformation configured")
        full_name = self.context.full_table_name(table)
        if table.name.lower() not in self._existing[self.context.full_schema_name(table)]:
            return TableResult.failure(
                table.qualified_name, f"{full_name} does not exist, run the schema service first"
            )
        injection = ContractInjection(
            self.context, table=table, contract=table.contract, data_product=table.data_product
        )
        result = injection.call(transformation)
        if result is None or not hasattr(result, "columns"):
            raise ContractError(f"The transformation of {table.qualified_name} must return a DataFrame.")
        overwrite(self.spark, conform_to_contract(result, table), full_name)
        rows = last_operation_metrics(self.spark, full_name).get("numOutputRows")
        written = f"{rows} row(s)" if rows is not None else "data"
        return TableResult.success(table.qualified_name, f"wrote {written} to {full_name}")
