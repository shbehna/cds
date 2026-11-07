"""Schema service: creates the schemas and tables described by the data contracts."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field

from cds.config import QualitySettings, SchemaSettings
from cds.data_context import DataProduct, Table
from cds.services.base import Service, TableResult
from cds.services.tables import add_columns, column_types, list_tables
from cds.sql import column_definition, create_table_statement, normalize_type, quote_name, string_literal

logger = logging.getLogger(__name__)


def contract_properties(table: Table) -> dict[str, str]:
    """Table properties recording the contract of a table."""
    document = table.contract.document
    return {
        "cds.contract.id": document.id if document else table.contract.name,
        "cds.contract.version": document.version if document else "",
        "cds.data_product": table.data_product.qualified_name,
    }


@dataclass
class SchemaDifferences:
    """Differences between the columns of a table and those of its contract."""

    missing: list[str] = field(default_factory=list)
    unexpected: list[str] = field(default_factory=list)
    retyped: list[str] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (self.missing or self.unexpected or self.retyped)

    @property
    def is_additive(self) -> bool:
        """Whether the contract only adds columns, which does not require a new table."""
        return bool(self.missing) and not (self.unexpected or self.retyped)

    def __str__(self) -> str:
        parts = []
        if self.missing:
            parts.append(f"columns missing from the table: {', '.join(self.missing)}")
        if self.unexpected:
            parts.append(f"columns missing from the contract: {', '.join(self.unexpected)}")
        if self.retyped:
            parts.append(f"types changed: {', '.join(self.retyped)}")
        return "; ".join(parts)


def compare_schema(table: Table, actual: dict[str, tuple[str, str]], ignored: Sequence[str] = ()) -> SchemaDifferences:
    """Compares the columns of a table with its contract.

    The order of the columns is not compared: services write by column name.

    Args:
        table: The table and its contract.
        actual: Columns of the table, as returned by ``column_types``.
        ignored: Columns managed by the framework, such as the quality results.
    """
    differences = SchemaDifferences()
    expected = {prop.column.lower(): prop for prop in table.columns}
    for key, prop in expected.items():
        if key not in actual:
            differences.missing.append(prop.column)
            continue
        contract_type, table_type = normalize_type(prop.data_type), normalize_type(actual[key][1])
        if contract_type != table_type:
            differences.retyped.append(f"{prop.column} ({table_type} -> {contract_type})")
    ignored_keys = {column.lower() for column in ignored}
    differences.unexpected = [
        name for key, (name, _) in actual.items() if key not in expected and key not in ignored_keys
    ]
    return differences


class SchemaService(Service):
    """Creates the schemas and tables of a data product from its contracts.

    A missing table is created. An existing table matching its contract is left
    untouched with its data. Columns added to a contract are added to the table. A
    table whose columns were removed or retyped is replaced, or reported in error,
    according to ``SchemaSettings.on_breaking_change``.

    Columns are created nullable: required columns are checked by the quality service.
    """

    name = "schema"

    def prepare(self, data_product: DataProduct, tables: Sequence[Table]) -> None:
        schemas: dict[str, Table] = {}
        for table in tables:
            schemas.setdefault(self.context.full_schema_name(table), table)
        comment = data_product.description or f"Data product {data_product.qualified_name}, managed by CDS."
        for full_schema_name in schemas:
            logger.info("Creating the schema %s if it does not exist", full_schema_name)
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {quote_name(full_schema_name)} COMMENT {string_literal(comment)}"
            )
        self._existing = {name: list_tables(self.spark, name) for name in schemas}

    def process(self, table: Table) -> TableResult:
        full_name = self.context.full_table_name(table)
        existing = self._existing[self.context.full_schema_name(table)]
        if table.name.lower() not in existing:
            self.spark.sql(self._create_statement(table, full_name))
            return TableResult.success(table.qualified_name, f"created {full_name} ({len(table.columns)} columns)")

        ignored = table.settings(QualitySettings).result_columns
        differences = compare_schema(table, column_types(self.spark, full_name), ignored)
        if differences.is_empty:
            return TableResult.success(table.qualified_name, f"{full_name} matches its contract")
        if differences.is_additive:
            added = [column_definition(prop) for prop in table.columns if prop.column in differences.missing]
            add_columns(self.spark, full_name, added)
            return TableResult.success(table.qualified_name, f"added to {full_name}: {', '.join(differences.missing)}")
        if table.settings(SchemaSettings).on_breaking_change == "error":
            return TableResult.failure(table.qualified_name, f"{full_name} does not match its contract: {differences}")
        self.spark.sql(self._create_statement(table, full_name, replace=True))
        return TableResult.warning(
            table.qualified_name,
            f"replaced {full_name}, its data was dropped (still reachable with Delta time travel): {differences}",
        )

    @staticmethod
    def _create_statement(table: Table, full_name: str, replace: bool = False) -> str:
        return create_table_statement(
            full_name,
            table.columns,
            description=table.description,
            partition_columns=table.schema_object.partition_columns,
            properties=contract_properties(table),
            replace=replace,
        )
