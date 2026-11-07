"""Snapshot service: loads tables with the latest record of each key of a historical table."""

from __future__ import annotations

from collections.abc import Sequence

from cds.config import ScdSettings, SnapshotSettings, resolved
from cds.data_context import DataProduct, Table
from cds.errors import ConfigurationError
from cds.services.base import Service, TableResult
from cds.services.tables import last_operation_metrics, list_tables
from cds.sql import column_list, quote_identifier, quote_name


def snapshot_source(table: Table, full_table_name: str) -> str:
    """Returns the unquoted name of the historical table of a snapshot table.

    Raises:
        ConfigurationError: If the configured source is neither a table name nor a
            fully qualified ``catalog.schema.table`` name.
    """
    pattern = table.settings(SnapshotSettings).source or resolved(table.settings(ScdSettings).table)
    name = pattern.format(table=table.name)
    parts = name.split(".")
    if len(parts) == 1:
        return f"{full_table_name.rsplit('.', 1)[0]}.{name}"
    if len(parts) == 3 and all(parts):
        return name
    raise ConfigurationError(
        f"{table.qualified_name}: invalid snapshot source {name!r}, expected a table name or catalog.schema.table."
    )


def snapshot_statement(table: Table, target: str, source: str) -> str:
    """Returns the statement replacing a table with the latest record of each key."""
    snapshot = table.settings(SnapshotSettings)
    valid_from = resolved(table.settings(ScdSettings).valid_from_column)
    order_by = snapshot.order_by or [f"{quote_identifier(valid_from)} DESC"]
    where = f" WHERE {snapshot.condition}" if snapshot.condition else ""
    return (
        f"INSERT OVERWRITE TABLE {quote_name(target)} BY NAME "
        f"SELECT {column_list(table.column_names)} FROM {quote_name(source)}{where} "
        f"QUALIFY row_number() OVER (PARTITION BY {column_list(table.primary_key)} "
        f"ORDER BY {', '.join(order_by)}) = 1"
    )


class SnapshotService(Service):
    """Loads tables with the latest record of each key of a historical table.

    The key is the primary key of the contract of the table. The historical table,
    the ordering defining the latest record and an optional filter come from
    ``SnapshotSettings``; by default, the history table of the SCD service ordered by
    start of validity. Tables whose snapshot is not enabled are skipped.
    """

    name = "snapshot"

    def prepare(self, data_product: DataProduct, tables: Sequence[Table]) -> None:
        schemas = {self.context.full_schema_name(table) for table in tables}
        self._existing = {name: list_tables(self.spark, name) for name in schemas}

    def process(self, table: Table) -> TableResult:
        if not table.settings(SnapshotSettings).enabled:
            return TableResult.skipped(table.qualified_name, "snapshot not enabled")
        if not table.primary_key:
            return TableResult.failure(table.qualified_name, "a snapshot requires a primary key in the contract")
        target = self.context.full_table_name(table)
        if table.name.lower() not in self._existing[self.context.full_schema_name(table)]:
            return TableResult.failure(table.qualified_name, f"{target} does not exist, run the schema service first")
        source = snapshot_source(table, target)
        self.spark.sql(snapshot_statement(table, target, source))
        rows = last_operation_metrics(self.spark, target).get("numOutputRows")
        return TableResult.success(
            table.qualified_name, f"loaded {rows if rows is not None else 'the'} latest record(s) of {source}"
        )
