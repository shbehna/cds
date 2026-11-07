"""Metadata service: documents the tables with the descriptions of their contracts."""

from __future__ import annotations

from collections.abc import Sequence

from cds.data_context import DataProduct, Table
from cds.services.base import Service, TableResult
from cds.services.schema import contract_properties
from cds.services.tables import list_tables
from cds.sql import quote_identifier, quote_name, string_literal, table_properties


class MetadataService(Service):
    """Applies the descriptions of the contracts as Unity Catalog comments.

    The table comment and the contract properties are set on each run; column
    comments only when they differ from their contract. Tables that do not exist are
    skipped: they are created by the schema service.
    """

    name = "metadata"

    def prepare(self, data_product: DataProduct, tables: Sequence[Table]) -> None:
        schemas = {self.context.full_schema_name(table) for table in tables}
        self._existing = {name: list_tables(self.spark, name) for name in schemas}

    def process(self, table: Table) -> TableResult:
        full_name = self.context.full_table_name(table)
        if table.name.lower() not in self._existing[self.context.full_schema_name(table)]:
            return TableResult.skipped(table.qualified_name, f"{full_name} does not exist")
        quoted = quote_name(full_name)
        fields = {field.name.lower(): field for field in self.spark.table(quoted).schema.fields}
        statements = []
        if table.description:
            statements.append(f"COMMENT ON TABLE {quoted} IS {string_literal(table.description)}")
        columns = 0
        for prop in table.columns:
            field = fields.get(prop.column.lower())
            if field is None or not prop.description:
                continue
            if (field.metadata or {}).get("comment") != prop.description:
                statements.append(
                    f"ALTER TABLE {quoted} ALTER COLUMN {quote_identifier(field.name)} "
                    f"COMMENT {string_literal(prop.description)}"
                )
                columns += 1
        statements.append(f"ALTER TABLE {quoted} SET TBLPROPERTIES ({table_properties(contract_properties(table))})")
        for statement in statements:
            self.spark.sql(statement)
        return TableResult.success(table.qualified_name, f"documented {full_name}, {columns} column comment(s) updated")
