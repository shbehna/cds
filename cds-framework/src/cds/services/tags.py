"""Unity Catalog tags of the tables and their columns, derived from their contracts.

The tags of a table come from the ``tags`` of its contract and schema object, from the
top-level fields of ``MetadataSettings.field_tags`` (such as ``domain``) and from the
certification of its contract ``status``. The tags of a column come from the ``tags``
and the ``classification`` of its property.

An ODCS tag ``key:value`` becomes the Unity Catalog tag ``key`` with the value
``value``; a tag without colon becomes a tag without value.

The metadata service only removes the tags it applied: it records their keys in the
``cds.tags`` table property, so that the tags set by other means, such as the automatic
data classification of Databricks, are left untouched.

The schema and SCD services tag the columns of the tables they create, and the columns
they add, before any data is written to them: the policies based on their tags, such as
a column mask, protect the data from its first write. The tags of the tables are left to
the metadata service.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cds.config import MetadataSettings, resolved
from cds.errors import ContractError
from cds.sql import set_tags_statement, tag_key_problem

if TYPE_CHECKING:
    from cds.data_context import Table

# Table property recording the keys of the tags applied by the metadata service.
MANAGED_TAGS_PROPERTY = "cds.tags"


@dataclass(frozen=True)
class TableTags:
    """Tags of a table and of its columns.

    Attributes:
        table: Tags of the table, key to value.
        columns: Tags of the columns, by column name.
    """

    table: Mapping[str, str] = field(default_factory=dict)
    columns: Mapping[str, Mapping[str, str]] = field(default_factory=dict)

    def keys(self) -> dict[str, Any]:
        """The keys of the tags, as recorded in the ``cds.tags`` table property."""
        return {
            "table": sorted(self.table),
            "columns": {column: sorted(tags) for column, tags in sorted(self.columns.items()) if tags},
        }


@dataclass(frozen=True)
class TagChanges:
    """Changes applying the expected tags of an object."""

    set: Mapping[str, str] = field(default_factory=dict)
    unset: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return bool(self.set or self.unset)


def parse_tag(tag: str, where: str) -> tuple[str, str]:
    """Converts an ODCS tag into the key and value of a Unity Catalog tag.

    Raises:
        ContractError: If the key is not a valid Unity Catalog tag key.
    """
    key, _, value = tag.partition(":")
    key, value = key.strip(), value.strip()
    problem = tag_key_problem(key)
    if problem:
        raise ContractError(f"{where}: invalid tag {tag!r}: {problem}.")
    return key, value


def expected_tags(table: Table) -> TableTags:
    """Returns the tags of a table and of its columns, derived from its contract.

    Raises:
        ContractError: If a tag of the contract is not a valid Unity Catalog tag.
    """
    settings = table.settings(MetadataSettings)
    document = table.contract.document
    raw: Mapping[str, Any] = document.raw if document else {}
    tags: dict[str, str] = {}
    for name, key in resolved(settings.field_tags).items():
        value = raw.get(name)
        if key and isinstance(value, (str, int, float)) and not isinstance(value, bool) and str(value).strip():
            tags[key] = str(value).strip()
    for tag in (*(document.tags if document else ()), *table.schema_object.tags):
        key, value = parse_tag(tag, table.qualified_name)
        tags[key] = value
    certification = certification_value(table)
    if certification:
        tags[resolved(settings.certification_tag)] = certification

    classification_tag = resolved(settings.classification_tag)
    columns: dict[str, dict[str, str]] = {}
    for prop in table.columns:
        column_tags: dict[str, str] = {}
        for tag in prop.tags:
            key, value = parse_tag(tag, f"{table.qualified_name}.{prop.column}")
            column_tags[key] = value
        if prop.classification:
            column_tags[classification_tag] = prop.classification
        if column_tags:
            columns[prop.column] = column_tags
    return TableTags(tags, columns)


def certification_value(table: Table) -> str | None:
    """Returns the value of the certification tag of a table, from the status of its contract."""
    certification = table.settings(MetadataSettings).certification
    document = table.contract.document
    if not certification or document is None or not document.status:
        return None
    by_status = {status.lower(): value for status, value in certification.items()}
    return by_status.get(document.status.lower()) or None


def tag_problems(table: Table) -> list[str]:
    """Returns the problems of the tags of a table, for ``cds validate``."""
    try:
        expected_tags(table)
    except ContractError as exc:
        return [str(exc)]
    allowed = table.settings(MetadataSettings).classifications
    if allowed is None:
        return []
    return [
        f"the classification {prop.classification!r} of {prop.column} is not one of {', '.join(allowed)}"
        for prop in table.columns
        if prop.classification and prop.classification not in allowed
    ]


def tag_changes(expected: Mapping[str, str], actual: Mapping[str, str], managed: Iterable[str]) -> TagChanges:
    """Returns the changes turning the actual tags of an object into the expected ones.

    Args:
        expected: The tags derived from the contract.
        actual: The tags of the object.
        managed: Keys of the tags applied by the previous runs: those no longer
            expected are removed. Other tags are left untouched.
    """
    to_set = {key: value for key, value in expected.items() if actual.get(key) != value}
    to_unset = tuple(sorted({key for key in managed if key not in expected and key in actual}))
    return TagChanges(to_set, to_unset)


def managed_keys(properties: Mapping[str, str]) -> tuple[set[str], dict[str, set[str]]]:
    """Returns the keys recorded in the ``cds.tags`` property: those of the table, and by column.

    A missing or unreadable property records no key.
    """
    try:
        recorded = json.loads(properties.get(MANAGED_TAGS_PROPERTY) or "{}")
    except ValueError:
        return set(), {}
    if not isinstance(recorded, Mapping):
        return set(), {}
    table = {str(key) for key in recorded.get("table") or [] if isinstance(key, str)}
    columns_value = recorded.get("columns")
    columns: dict[str, set[str]] = {}
    if isinstance(columns_value, Mapping):
        for column, keys in columns_value.items():
            if isinstance(keys, list):
                columns[str(column).lower()] = {key for key in keys if isinstance(key, str)}
    return table, columns


def managed_property(tags: TableTags) -> str:
    """Returns the value of the ``cds.tags`` property recording the keys of the applied tags."""
    return json.dumps(tags.keys(), separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def restricted(tags: TableTags, columns: Mapping[str, str]) -> TableTags:
    """Returns the tags of a table, restricted to the columns it has, named as in it.

    Args:
        tags: The tags of the table and of its columns.
        columns: The columns of the table, lowercase name to name.
    """
    applied = {
        columns[column.lower()]: column_tags
        for column, column_tags in tags.columns.items()
        if column.lower() in columns
    }
    return TableTags(tags.table, applied)


def creation_tags(table: Table, columns: Iterable[str]) -> TableTags | None:
    """Returns the tags of new columns, to apply before writing data to them.

    The tags of the table, such as its certification, are not included: they do not
    protect data, and are left to the metadata service.

    Args:
        table: The table whose contract gives the tags.
        columns: The new columns.

    Returns:
        The tags of the columns, or ``None`` if the tags of the table are not synchronized
        (``MetadataSettings.tags``).

    Raises:
        ContractError: If a tag of the contract is not a valid Unity Catalog tag.
    """
    if not table.settings(MetadataSettings).tags:
        return None
    expected = restricted(expected_tags(table), {column.lower(): column for column in columns})
    return TableTags({}, expected.columns)


def set_tags_statements(full_table_name: str, tags: TableTags) -> list[str]:
    """Returns the statements setting tags on a table and its columns, which do not have them yet."""
    statements = [set_tags_statement(full_table_name, tags.table)] if tags.table else []
    statements += [
        set_tags_statement(full_table_name, column_tags, column=column)
        for column, column_tags in sorted(tags.columns.items())
        if column_tags
    ]
    return statements


def tag_count(tags: TableTags) -> int:
    """Returns the number of tags of a table and of its columns."""
    return len(tags.table) + sum(len(column_tags) for column_tags in tags.columns.values())
