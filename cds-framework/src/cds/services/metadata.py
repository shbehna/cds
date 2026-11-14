"""Metadata service: documents the tables with the descriptions and tags of their contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from cds.config import MetadataSettings, ScdSettings
from cds.data_context import DataProduct, Table
from cds.services.base import Service, TableResult
from cds.services.scd import history_table_name
from cds.services.schema import contract_properties
from cds.services.tables import list_tables
from cds.services.tags import (
    MANAGED_TAGS_PROPERTY,
    TableTags,
    expected_tags,
    managed_keys,
    managed_property,
    restricted,
    tag_changes,
)
from cds.sql import (
    quote_identifier,
    quote_name,
    set_tags_statement,
    string_literal,
    table_properties,
    unset_tags_statement,
)

# Tags of the objects of a schema: lowercase table name to tags, and to tags by lowercase column name.
TableTagsByName = dict[str, dict[str, str]]
ColumnTagsByName = dict[str, dict[str, dict[str, str]]]


class MetadataService(Service):
    """Applies the descriptions and tags of the contracts to the Unity Catalog objects.

    The table comment and the contract properties are set on each run; column
    comments only when they differ from their contract. The tags of the tables and
    columns are synchronized with their contracts (see ``cds.services.tags``) when
    ``MetadataSettings.tags`` is set. The history table of a table gets the tags of its
    columns, such as their classification, so that the policies based on them also
    protect the history. Tables that do not exist are skipped: they are created, with the
    tags of their columns, by the schema service, and the history tables by the SCD service.
    """

    name = "metadata"

    def prepare(self, data_product: DataProduct, tables: Sequence[Table]) -> None:
        schemas = {self.context.full_schema_name(table) for table in tables}
        self._existing = {name: list_tables(self.spark, name) for name in schemas}
        tagged = {self.context.full_schema_name(table) for table in tables if table.settings(MetadataSettings).tags}
        self._table_tags: dict[str, TableTagsByName] = {}
        self._column_tags: dict[str, ColumnTagsByName] = {}
        for full_schema_name in sorted(tagged):
            if self._existing[full_schema_name]:
                self._table_tags[full_schema_name], self._column_tags[full_schema_name] = self._read_tags(
                    full_schema_name
                )

    def process(self, table: Table) -> TableResult:
        full_name = self.context.full_table_name(table)
        full_schema_name = self.context.full_schema_name(table)
        if table.name.lower() not in self._existing[full_schema_name]:
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
        message = f"documented {full_name}, {columns} column comment(s) updated"

        properties = contract_properties(table)
        history: list[str] = []
        if table.settings(MetadataSettings).tags:
            expected = expected_tags(table)
            names = {name: field.name for name, field in fields.items()}
            applied = restricted(expected, names)
            tag_statements, changed = self._tag_statements(full_schema_name, table.name, full_name, applied, names)
            statements += tag_statements
            properties[MANAGED_TAGS_PROPERTY] = managed_property(applied)
            history, history_changed = self._history_statements(table, expected)
            message += f", {changed + history_changed} tag(s) set or removed"
            if history:
                message += ", history table included"
        # The recorded tags are those applied by the statements above.
        statements.append(f"ALTER TABLE {quoted} SET TBLPROPERTIES ({table_properties(properties)})")
        for statement in [*statements, *history]:
            self.spark.sql(statement)
        return TableResult.success(table.qualified_name, message)

    def _read_tags(self, full_schema_name: str) -> tuple[TableTagsByName, ColumnTagsByName]:
        catalog, schema = full_schema_name.split(".")
        information_schema = f"{quote_identifier(catalog)}.`information_schema`"
        condition = f"WHERE schema_name = {string_literal(schema)}"
        table_tags: TableTagsByName = {}
        for row in self.spark.sql(
            f"SELECT table_name, tag_name, tag_value FROM {information_schema}.`table_tags` {condition}"
        ).collect():
            table_tags.setdefault(row.table_name.lower(), {})[row.tag_name] = row.tag_value or ""
        column_tags: ColumnTagsByName = {}
        for row in self.spark.sql(
            f"SELECT table_name, column_name, tag_name, tag_value FROM {information_schema}.`column_tags` {condition}"
        ).collect():
            by_column = column_tags.setdefault(row.table_name.lower(), {})
            by_column.setdefault(row.column_name.lower(), {})[row.tag_name] = row.tag_value or ""
        return table_tags, column_tags

    def _history_statements(self, table: Table, expected: TableTags) -> tuple[list[str], int]:
        """The statements applying the column tags of a table to its history table, if it exists."""
        if not table.settings(ScdSettings).enabled or not table.primary_key:
            return [], 0
        full_schema_name = self.context.full_schema_name(table)
        history = history_table_name(table)
        if history.lower() not in self._existing[full_schema_name]:
            return [], 0
        full_name = self.context.full_table_name(table, history)
        names = {field.name.lower(): field.name for field in self.spark.table(quote_name(full_name)).schema.fields}
        applied = restricted(TableTags({}, expected.columns), names)
        statements, changed = self._tag_statements(full_schema_name, history, full_name, applied, names)
        properties = {MANAGED_TAGS_PROPERTY: managed_property(applied)}
        statements.append(f"ALTER TABLE {quote_name(full_name)} SET TBLPROPERTIES ({table_properties(properties)})")
        return statements, changed

    def _tag_statements(
        self, full_schema_name: str, table_name: str, full_name: str, applied: TableTags, columns: Mapping[str, str]
    ) -> tuple[list[str], int]:
        """The statements applying the tags of a table and of its columns, and the number of tags they change.

        Args:
            full_schema_name: The schema of the table.
            table_name: Its name.
            full_name: Its unquoted full name.
            applied: Its expected tags, restricted to its columns.
            columns: Its columns, lowercase name to name.
        """
        key = table_name.lower()
        actual_table = self._table_tags.get(full_schema_name, {}).get(key, {})
        actual_columns = self._column_tags.get(full_schema_name, {}).get(key, {})
        managed_table, managed_columns = managed_keys(self._properties(full_name))

        statements = []
        changed = 0
        changes = tag_changes(applied.table, actual_table, managed_table)
        changed += len(changes.set) + len(changes.unset)
        if changes.set:
            statements.append(set_tags_statement(full_name, changes.set))
        if changes.unset:
            statements.append(unset_tags_statement(full_name, changes.unset))
        expected_columns = {column.lower(): tags for column, tags in applied.columns.items()}
        for lowered in sorted(set(expected_columns) | set(managed_columns)):
            column = columns.get(lowered)
            if column is None:
                continue
            changes = tag_changes(
                expected_columns.get(lowered, {}), actual_columns.get(lowered, {}), managed_columns.get(lowered, set())
            )
            changed += len(changes.set) + len(changes.unset)
            if changes.set:
                statements.append(set_tags_statement(full_name, changes.set, column=column))
            if changes.unset:
                statements.append(unset_tags_statement(full_name, changes.unset, column=column))
        return statements, changed

    def _properties(self, full_name: str) -> dict[str, str]:
        rows = self.spark.sql(f"SHOW TBLPROPERTIES {quote_name(full_name)}").collect()
        return {row.key: row.value for row in rows}
