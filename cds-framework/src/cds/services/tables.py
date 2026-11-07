"""Operations on Delta tables shared by the services."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from cds.sql import quote_identifier, quote_name

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def list_tables(spark: SparkSession, full_schema_name: str) -> set[str]:
    """Returns the lowercase names of the tables of a schema; empty if it does not exist."""
    try:
        rows = spark.sql(f"SHOW TABLES IN {quote_name(full_schema_name)}").collect()
    except Exception as exc:
        logger.info("Cannot list the tables of %s: %s", full_schema_name, exc)
        return set()
    return {row.tableName.lower() for row in rows if not getattr(row, "isTemporary", False)}


def column_types(spark: SparkSession, full_table_name: str) -> dict[str, tuple[str, str]]:
    """Returns the columns of a table: lowercase name to (name, Spark type)."""
    fields = spark.table(quote_name(full_table_name)).schema.fields
    return {field.name.lower(): (field.name, field.dataType.simpleString()) for field in fields}


def overwrite(spark: SparkSession, df: DataFrame, full_table_name: str) -> None:
    """Replaces the content of an existing table, keeping its definition.

    ``INSERT OVERWRITE ... BY NAME`` matches the columns by name and preserves the
    schema, comments, properties and permissions of the table. Columns of the table
    missing from the DataFrame are set to null.
    """
    view = f"cds_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    try:
        spark.sql(f"INSERT OVERWRITE TABLE {quote_name(full_table_name)} BY NAME SELECT * FROM {view}")
    finally:
        spark.catalog.dropTempView(view)


def last_operation_metrics(spark: SparkSession, full_table_name: str) -> Mapping[str, str]:
    """Returns the metrics of the last operation on a table, from its Delta history."""
    try:
        rows = spark.sql(f"DESCRIBE HISTORY {quote_name(full_table_name)} LIMIT 1").collect()
        return dict(rows[0].operationMetrics or {}) if rows else {}
    except Exception as exc:
        logger.debug("Cannot read the history of %s: %s", full_table_name, exc)
        return {}


def add_columns(spark: SparkSession, full_table_name: str, definitions: Sequence[str]) -> None:
    """Adds columns to a table, from their SQL definitions."""
    if definitions:
        spark.sql(f"ALTER TABLE {quote_name(full_table_name)} ADD COLUMNS ({', '.join(definitions)})")


def column_definition(name: str, data_type: str) -> str:
    return f"{quote_identifier(name)} {data_type}"
