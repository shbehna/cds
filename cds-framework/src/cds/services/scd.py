"""SCD service: keeps the history of the tables (slowly changing dimension, type 2)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from cds.config import ScdSettings, resolved
from cds.data_context import DataProduct, Table
from cds.odcs import SchemaProperty
from cds.services.base import Service, TableResult
from cds.services.schema import contract_properties
from cds.services.tables import add_columns, column_definition, last_operation_metrics, list_tables
from cds.services.tags import MANAGED_TAGS_PROPERTY, TableTags, creation_tags, managed_property, set_tags_statements
from cds.sla import Duration, duration, table_sla
from cds.sql import column_list, create_table_statement, normalize_type, quote_identifier, quote_name, string_literal

if TYPE_CHECKING:
    from cds.context import ServiceContext

# Marks the source rows of the merge that may match a current version.
_MATCH_COLUMN = "__cds_match"


def history_table_name(table: Table) -> str:
    """Returns the name of the history table of a table."""
    return resolved(table.settings(ScdSettings).table).format(table=table.name)


def history_columns(settings: ScdSettings) -> list[tuple[str, str]]:
    """Returns the technical columns of a history table, with their types."""
    return [
        (resolved(settings.hash_column), "STRING"),
        (resolved(settings.valid_from_column), "TIMESTAMP"),
        (resolved(settings.valid_to_column), "TIMESTAMP"),
        (resolved(settings.current_column), "BOOLEAN"),
    ]


def row_hash_expression(columns: Sequence[SchemaProperty]) -> str:
    """Returns the SQL expression hashing the tracked columns of a row.

    The values are serialized to JSON, which separates them unambiguously; timestamps are
    converted to microseconds, so that the hash does not depend on the session time zone.
    """
    if not columns:
        return "sha2('', 256)"
    members = ", ".join(
        f"{string_literal(prop.column)}, "
        + (
            f"unix_micros({quote_identifier(prop.column)})"
            if normalize_type(prop.data_type) == "timestamp"
            else quote_identifier(prop.column)
        )
        for prop in columns
    )
    return f"sha2(to_json(named_struct({members})), 256)"


def _key_condition(keys: Sequence[str], left: str, right: str) -> str:
    return " AND ".join(f"{left}.{quote_identifier(key)} <=> {right}.{quote_identifier(key)}" for key in keys)


def source_query(
    source: str, keys: Sequence[str], columns: Sequence[SchemaProperty], settings: ScdSettings, deduplicate: bool
) -> str:
    """Returns the query of the source rows of the merge, with their hash."""
    tracked = [prop for prop in columns if prop.column not in keys]
    hash_column = quote_identifier(resolved(settings.hash_column))
    query = (
        f"SELECT {column_list(prop.column for prop in columns)}, "
        f"{row_hash_expression(tracked)} AS {hash_column} FROM {quote_name(source)}"
    )
    if deduplicate:
        query = (
            f"SELECT * FROM ({query}) AS d QUALIFY row_number() OVER "
            f"(PARTITION BY {column_list(keys)} ORDER BY {hash_column}) = 1"
        )
    return query


def merge_statement(
    *,
    source: str,
    target: str,
    keys: Sequence[str],
    columns: Sequence[SchemaProperty],
    settings: ScdSettings,
    timestamp: datetime,
    deduplicate: bool = False,
) -> str:
    """Returns the MERGE statement applying the source rows to a history table.

    In a single statement, it closes the current version of the keys whose tracked
    columns changed and of the keys missing from the source, and inserts a new version
    for the new and changed keys. The changed keys are fed twice to the merge: once to
    match (and close) their current version, once to insert their new version.
    """
    rows = source_query(source, keys, columns, settings, deduplicate)
    hash_column = quote_identifier(resolved(settings.hash_column))
    current = quote_identifier(resolved(settings.current_column))
    valid_to = quote_identifier(resolved(settings.valid_to_column))
    match = quote_identifier(_MATCH_COLUMN)
    at = _timestamp(timestamp)
    names = [prop.column for prop in columns]
    # The values below follow the order of history_columns: hash, valid from, valid to, current.
    inserted = [*names, *(name for name, _ in history_columns(settings))]
    return f"""MERGE INTO {quote_name(target)} AS t
USING (
  SELECT s.*, FALSE AS {match} FROM ({rows}) AS s
  INNER JOIN {quote_name(target)} AS h ON {_key_condition(keys, "s", "h")} AND h.{current}
  WHERE s.{hash_column} <> h.{hash_column}
  UNION ALL
  SELECT s.*, TRUE AS {match} FROM ({rows}) AS s
) AS s
ON {_key_condition(keys, "t", "s")} AND t.{current} AND s.{match}
WHEN MATCHED AND t.{hash_column} <> s.{hash_column} THEN
  UPDATE SET t.{valid_to} = {at}, t.{current} = FALSE
WHEN NOT MATCHED THEN
  INSERT ({column_list(inserted)})
  VALUES ({column_list(names, "s")}, s.{hash_column}, {at}, NULL, TRUE)
WHEN NOT MATCHED BY SOURCE AND t.{current} THEN
  UPDATE SET t.{valid_to} = {at}, t.{current} = FALSE"""


def _timestamp(timestamp: datetime) -> str:
    """The SQL expression of a time, independent of the session time zone."""
    return f"timestamp_micros({int(timestamp.timestamp() * 1_000_000)})"


def retention_statement(target: str, settings: ScdSettings, timestamp: datetime, retention: Duration) -> str:
    """Returns the statement deleting the versions of a history table closed for longer than its retention.

    The current versions are kept, however old.
    """
    current = quote_identifier(resolved(settings.current_column))
    valid_to = quote_identifier(resolved(settings.valid_to_column))
    return (
        f"DELETE FROM {quote_name(target)} WHERE NOT {current} AND {valid_to} < {_timestamp(timestamp)} - "
        f"{retention.sql()}"
    )


def duplicate_keys_query(source: str, keys: Sequence[str]) -> str:
    """Returns the query counting the keys with several rows in the source."""
    return (
        f"SELECT count(*) AS duplicate_keys FROM (SELECT 1 FROM {quote_name(source)} "
        f"GROUP BY {column_list(keys)} HAVING count(*) > 1)"
    )


class ScdService(Service):
    """Historizes the tables of a data product, using the primary key of their contract.

    Each table with a primary key gets a history table (``{table}_history`` by default)
    with the columns of its contract and four technical columns: the hash of the tracked
    columns, the start and end of validity of the version, and whether it is the current
    version. Tables without a primary key are skipped. Columns added to a contract are
    added to its history table.

    The columns of a history table get the tags of the columns of its table, such as their
    classification, when they are created and before any version is written: the policies
    based on the tags also protect the history. A new history table whose columns cannot
    be tagged is dropped.

    The ``retention`` SLA of a table (see ``cds.sla``) applies to its history: each run
    deletes the versions closed for longer, and reports their number. Without retention,
    the history keeps every version.

    Args:
        context: The service context.
        timestamp: Start of validity of the versions created by the run; now by default.
    """

    name = "scd"

    def __init__(self, context: ServiceContext, timestamp: datetime | None = None) -> None:
        super().__init__(context)
        self._timestamp = timestamp

    def prepare(self, data_product: DataProduct, tables: Sequence[Table]) -> None:
        schemas = {self.context.full_schema_name(table) for table in tables}
        self._existing = {name: list_tables(self.spark, name) for name in schemas}
        self._run_timestamp = self._timestamp or datetime.now(timezone.utc)

    def process(self, table: Table) -> TableResult:
        settings = table.settings(ScdSettings)
        if not settings.enabled:
            return TableResult.skipped(table.qualified_name, "history disabled")
        keys = table.primary_key
        if not keys:
            return TableResult.skipped(table.qualified_name, "no primary key in the contract")
        existing = self._existing[self.context.full_schema_name(table)]
        source = self.context.full_table_name(table)
        if table.name.lower() not in existing:
            return TableResult.skipped(table.qualified_name, f"{source} does not exist")
        history = history_table_name(table)
        target = self.context.full_table_name(table, history)
        self._prepare_history(table, target, settings, exists=history.lower() in existing)

        duplicates = self.spark.sql(duplicate_keys_query(source, keys)).collect()[0]["duplicate_keys"]
        if duplicates and settings.on_duplicate_keys == "fail":
            return TableResult.failure(
                table.qualified_name,
                f"{duplicates} key(s) have several rows in {source}; fix the data or set "
                "ScdSettings(on_duplicate_keys='deduplicate')",
            )
        self.spark.sql(
            merge_statement(
                source=source,
                target=target,
                keys=keys,
                columns=table.columns,
                settings=settings,
                timestamp=self._run_timestamp,
                deduplicate=bool(duplicates),
            )
        )
        metrics = last_operation_metrics(self.spark, target)
        message = (
            f"{metrics.get('numTargetRowsInserted', '?')} version(s) inserted and "
            f"{metrics.get('numTargetRowsUpdated', '?')} closed in {target}"
        )
        retention = table_sla(table, "retention")
        if retention is not None:
            period = duration(retention[0])
            rows = self.spark.sql(retention_statement(target, settings, self._run_timestamp, period)).collect()
            purged = rows[0]["num_affected_rows"] if rows else 0
            message += f", {purged} version(s) closed for more than {period} purged"
        if duplicates:
            return TableResult.warning(
                table.qualified_name, f"{message}; kept one row for each of {duplicates} duplicate key(s)"
            )
        return TableResult.success(table.qualified_name, message)

    def _prepare_history(self, table: Table, target: str, settings: ScdSettings, exists: bool) -> None:
        """Creates the history table, or adds the columns added to the contract, with their tags."""
        if not exists:
            columns = [prop.column for prop in table.columns]
            tags = creation_tags(table, columns)
            properties = contract_properties(table)
            if tags is not None:
                properties[MANAGED_TAGS_PROPERTY] = managed_property(tags)
            self.spark.sql(
                create_table_statement(
                    target,
                    table.columns,
                    description=f"History (SCD type 2) of {table.qualified_name}.",
                    properties=properties,
                    extra_columns=history_columns(settings),
                )
            )
            try:
                self._set_tags(target, tags)
            except Exception:
                # The new history table is still empty.
                self.spark.sql(f"DROP TABLE IF EXISTS {quote_name(target)}")
                raise
            return
        existing = {field.name.lower() for field in self.spark.table(quote_name(target)).schema.fields}
        missing = [
            (name, data_type)
            for name, data_type in [(prop.column, prop.data_type) for prop in table.columns] + history_columns(settings)
            if name.lower() not in existing
        ]
        add_columns(self.spark, target, [column_definition(name, data_type) for name, data_type in missing])
        self._set_tags(target, creation_tags(table, [name for name, _ in missing]))

    def _set_tags(self, target: str, tags: TableTags | None) -> None:
        for statement in set_tags_statements(target, tags) if tags else []:
            self.spark.sql(statement)
