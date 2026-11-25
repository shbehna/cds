"""Business semantics of the contracts: the synonyms and measures of their custom properties.

ODCS has no field for the other names of a concept, nor for the measures of a table:
the contracts declare them as custom properties, which the Pages (``cds.pages``) and the
metric views (``cds.metric_views``) read.

- ``synonyms``, on a schema object or a property: a list of strings, the other names of
  the table or the column, such as ``[Assujetti, Compagnie]``.
- ``measures``, on a schema object: a list of mappings, the aggregations of the table,
  each with a ``name``, a SQL aggregate ``expr``, and optionally a ``description``, a
  ``businessName``, ``synonyms`` and a ``format`` (see ``Measure``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cds.config import MetricViewSettings, resolved
from cds.errors import ContractError

if TYPE_CHECKING:
    from cds.data_context import Table

SYNONYMS_PROPERTY = "synonyms"
MEASURES_PROPERTY = "measures"
# Limits of the Pages API, the most permissive consumer of the synonyms.
MAX_SYNONYMS = 50
MAX_SYNONYM_LENGTH = 200
# Keys of a measure, and the types of its format in a metric view.
MEASURE_KEYS = frozenset({"name", "expr", "description", "businessName", "synonyms", "format"})
FORMAT_TYPES = ("number", "currency", "percentage", "byte", "date", "date_time")


@dataclass(frozen=True)
class Measure:
    """A measure of a table: an aggregation of its rows, such as a count or a sum.

    Attributes:
        name: Its name in the metric view of the table, unique among its fields.
        expr: Its SQL expression, with aggregate functions, on the columns of the table
            and of the tables it joins.
        description: What it measures.
        business_name: Its name for the users (``display_name`` in the metric view).
        synonyms: Its other names, sorted.
        format: How to display its values, as the metric view YAML describes it, such as
            ``{"type": "number"}``.
    """

    name: str
    expr: str
    description: str | None = None
    business_name: str | None = None
    synonyms: tuple[str, ...] = ()
    format: Mapping[str, Any] = field(default_factory=dict)


def synonyms(custom_properties: Mapping[str, Any], where: str) -> tuple[str, ...]:
    """The synonyms of a schema object or a property: its ``synonyms`` custom property, trimmed, sorted.

    Raises:
        ContractError: If it is not a list of strings, or exceeds the limits of the Pages API.
    """
    return _synonyms(custom_properties.get(SYNONYMS_PROPERTY), where)


def _synonyms(value: object, where: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence) or not all(isinstance(item, str) for item in value):
        raise ContractError(f"{where}: the {SYNONYMS_PROPERTY} custom property must be a list of strings.")
    return checked_synonyms(value, where)


def checked_synonyms(values: Iterable[str], where: str) -> tuple[str, ...]:
    """Synonyms trimmed, deduplicated and sorted, as the Pages API returns them.

    Raises:
        ContractError: If a synonym is empty or too long, or there are too many.
    """
    found = sorted({value.strip() for value in values})
    for value in found:
        if not value or len(value) > MAX_SYNONYM_LENGTH:
            raise ContractError(
                f"{where}: a synonym must have from 1 to {MAX_SYNONYM_LENGTH} characters, got {value!r}."
            )
    if len(found) > MAX_SYNONYMS:
        raise ContractError(f"{where}: more than {MAX_SYNONYMS} synonyms.")
    return tuple(found)


def measures(custom_properties: Mapping[str, Any], where: str) -> tuple[Measure, ...]:
    """The measures of a schema object: its ``measures`` custom property.

    Raises:
        ContractError: If it is not a list of measures, or a measure is invalid.
    """
    value = custom_properties.get(MEASURES_PROPERTY)
    if value is None:
        return ()
    if isinstance(value, (str, Mapping)) or not isinstance(value, Sequence):
        raise ContractError(f"{where}: the {MEASURES_PROPERTY} custom property must be a list of measures.")
    return tuple(_measure(item, where, index) for index, item in enumerate(value))


def table_measures(table: Table) -> tuple[Measure, ...]:
    """The measures of a table, declared by its schema object.

    Raises:
        ContractError: If a measure is invalid.
    """
    return measures(table.schema_object.custom_properties, table.qualified_name)


def metric_view_name(table: Table) -> str | None:
    """The name of the metric view of a table (``MetricViewSettings.name``); ``None`` if it has none.

    A table has a metric view when the metric views are enabled and its contract declares
    measures.

    Raises:
        ContractError: If a measure is invalid.
    """
    settings = table.settings(MetricViewSettings)
    if not settings.enabled or not table_measures(table):
        return None
    return resolved(settings.name).format(table=table.name)


def _measure(item: object, owner: str, index: int) -> Measure:
    where = f"{owner}: measure {index + 1}"
    if not isinstance(item, Mapping):
        raise ContractError(f"{where} must be a mapping with a name and an expr.")
    unknown = sorted(str(key) for key in set(item) - MEASURE_KEYS)
    if unknown:
        expected = ", ".join(sorted(MEASURE_KEYS))
        raise ContractError(f"{where}: unknown key(s) {', '.join(unknown)}, expected {expected}.")
    name, expr = _text(item, "name", where), _text(item, "expr", where)
    if not name or not expr:
        raise ContractError(f"{where} needs a name and an expr.")
    where = f"{owner}: measure {name!r}"
    measure_format = item.get("format") or {}
    if measure_format and (not isinstance(measure_format, Mapping) or measure_format.get("type") not in FORMAT_TYPES):
        raise ContractError(f"{where}: its format needs a type, one of {', '.join(FORMAT_TYPES)}.")
    return Measure(
        name=name,
        expr=expr,
        description=_text(item, "description", where),
        business_name=_text(item, "businessName", where),
        synonyms=_synonyms(item.get("synonyms"), where),
        format=dict(measure_format),
    )


def _text(item: Mapping[str, Any], key: str, where: str) -> str | None:
    value = item.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractError(f"{where}: {key} must be a string, got {value!r}.")
    return value.strip() or None
