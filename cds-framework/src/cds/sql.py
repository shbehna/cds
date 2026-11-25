"""SQL generation helpers (Databricks SQL dialect)."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from cds.odcs import SchemaProperty

# Aliases of Spark SQL types, normalized to the names Spark reports in table schemas.
_TYPE_ALIASES = {
    "integer": "int",
    "long": "bigint",
    "short": "smallint",
    "byte": "tinyint",
    "real": "float",
    "bool": "boolean",
    "text": "string",
    "timestamp_ltz": "timestamp",
    "dec": "decimal",
    "numeric": "decimal",
}
_ALIAS_PATTERN = re.compile(r"\b(" + "|".join(_TYPE_ALIASES) + r")\b")

# Names of the Spark JSON representation of types that differ from their SQL names.
_JSON_TYPE_NAMES = {"integer": "INT", "long": "BIGINT", "short": "SMALLINT", "byte": "TINYINT"}


def quote_identifier(name: str) -> str:
    """Quotes an identifier with backticks."""
    return "`" + name.replace("`", "``") + "`"


def quote_name(full_name: str) -> str:
    """Quotes each part of a dotted name, e.g. ``catalog.schema.table``."""
    return ".".join(quote_identifier(part) for part in full_name.split("."))


def string_literal(value: str) -> str:
    """Returns a SQL string literal (Databricks escapes with backslashes)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def column_list(columns: Iterable[str], prefix: str = "") -> str:
    """Returns a comma-separated list of quoted columns, optionally qualified by an alias."""
    qualifier = f"{prefix}." if prefix else ""
    return ", ".join(f"{qualifier}{quote_identifier(column)}" for column in columns)


def normalize_type(type_name: str) -> str:
    """Normalizes a type name to compare a contract type with a table type.

    Case, spaces and aliases (``INTEGER``, ``LONG``...) are normalized. ``VARCHAR(n)``
    and ``CHAR(n)`` become ``string``, as Spark reports them, and ``DECIMAL`` gets its
    default precision.
    """
    text = "".join(type_name.split()).lower()
    text = re.sub(r"(?<![a-z_])(var)?char\(\d+\)", "string", text)
    text = _ALIAS_PATTERN.sub(lambda match: _TYPE_ALIASES[match.group(1)], text)
    text = re.sub(r"decimal(?!\()", "decimal(10,0)", text)
    return re.sub(r"decimal\((\d+)\)", r"decimal(\1,0)", text)


def ddl_type(data_type: str | Mapping[str, Any]) -> str:
    """Returns the SQL type of a Spark type, from its JSON representation.

    Args:
        data_type: The ``jsonValue()`` of a Spark ``DataType``.
    """
    if isinstance(data_type, str):
        return _JSON_TYPE_NAMES.get(data_type, data_type.upper())
    kind = data_type["type"]
    if kind == "array":
        return f"ARRAY<{ddl_type(data_type['elementType'])}>"
    if kind == "map":
        return f"MAP<{ddl_type(data_type['keyType'])}, {ddl_type(data_type['valueType'])}>"
    if kind == "struct":
        members = ", ".join(
            f"{quote_identifier(member['name'])}: {ddl_type(member['type'])}" for member in data_type["fields"]
        )
        return f"STRUCT<{members}>"
    raise ValueError(f"Unsupported Spark type {data_type!r}")


def column_definition(prop: SchemaProperty) -> str:
    """Returns the definition of a column of a contract, for CREATE or ALTER TABLE."""
    definition = f"{quote_identifier(prop.column)} {prop.data_type}"
    if prop.description:
        definition += f" COMMENT {string_literal(prop.description)}"
    return definition


def table_properties(properties: Mapping[str, str]) -> str:
    return ", ".join(f"{string_literal(key)} = {string_literal(value)}" for key, value in properties.items())


def create_table_statement(
    full_name: str,
    columns: Sequence[SchemaProperty],
    *,
    description: str | None = None,
    partition_columns: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    extra_columns: Sequence[tuple[str, str]] = (),
    replace: bool = False,
) -> str:
    """Returns the statement creating a Delta table.

    Columns are nullable: required columns are enforced by the quality service, which
    reports each faulty row rather than failing the whole write.

    Args:
        full_name: Unquoted ``catalog.schema.table`` name.
        columns: Columns of the contract.
        description: Comment of the table.
        partition_columns: Partition columns.
        properties: Table properties.
        extra_columns: Additional ``(name, type)`` columns, after those of the contract.
        replace: ``CREATE OR REPLACE`` instead of ``CREATE ... IF NOT EXISTS``.
    """
    definitions = [column_definition(prop) for prop in columns]
    definitions += [f"{quote_identifier(name)} {data_type}" for name, data_type in extra_columns]
    verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    statement = f"{verb} {quote_name(full_name)} (\n  " + ",\n  ".join(definitions) + "\n)\nUSING DELTA"
    if partition_columns:
        statement += f"\nPARTITIONED BY ({column_list(partition_columns)})"
    if description:
        statement += f"\nCOMMENT {string_literal(description)}"
    if properties:
        statement += f"\nTBLPROPERTIES ({table_properties(properties)})"
    return statement


# Characters Unity Catalog does not allow in the key of a tag.
_TAG_KEY_FORBIDDEN = re.compile(r"[.,\-=/:]")
TAG_KEY_MAX_LENGTH = 255


def tag_key_problem(key: str) -> str | None:
    """Returns why a string cannot be the key of a Unity Catalog tag, or ``None`` if it can."""
    if not key or not key.strip():
        return "a tag key cannot be empty"
    if key != key.strip():
        return f"the tag key {key!r} has leading or trailing spaces"
    if len(key) > TAG_KEY_MAX_LENGTH:
        return f"the tag key {key!r} is longer than {TAG_KEY_MAX_LENGTH} characters"
    forbidden = _TAG_KEY_FORBIDDEN.search(key)
    if forbidden:
        return f"the tag key {key!r} contains {forbidden.group()!r}, not allowed in Unity Catalog (. , - = / :)"
    return None


def set_tags_statement(
    full_table_name: str, tags: Mapping[str, str], column: str | None = None, kind: str = "TABLE"
) -> str:
    """Returns the statement setting tags on a table (or a ``VIEW``, by ``kind``), or on one of its columns."""
    assignments = ", ".join(f"{string_literal(key)} = {string_literal(value)}" for key, value in tags.items())
    return f"{_tags_target(full_table_name, column, kind)} SET TAGS ({assignments})"


def unset_tags_statement(
    full_table_name: str, keys: Iterable[str], column: str | None = None, kind: str = "TABLE"
) -> str:
    """Returns the statement removing tags from a table (or a ``VIEW``, by ``kind``), or from one of its columns."""
    keys_list = ", ".join(string_literal(key) for key in keys)
    return f"{_tags_target(full_table_name, column, kind)} UNSET TAGS ({keys_list})"


def _tags_target(full_table_name: str, column: str | None, kind: str) -> str:
    target = f"ALTER {kind} {quote_name(full_table_name)}"
    return f"{target} ALTER COLUMN {quote_identifier(column)}" if column else target
