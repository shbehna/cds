"""Metric views of the tables, generated from their contracts.

A metric view is a Unity Catalog view defined in YAML: a source table, the tables it
joins, its dimensions and its measures, with their names for the users and their
synonyms. Genie, the AI/BI dashboards and Genie Ontology read it as the governed
definition of the measures of a table.

A table whose contract declares measures (the ``measures`` custom property of its schema
object, see ``cds.semantics``) has a metric view (``MetricViewSettings``):

- its ``source`` is the table, and its ``comment`` the description of the table;
- its ``joins`` are the ``foreignKey`` relationships of the table to the tables of its
  data product whose key they reference (see ``cds.genie.joins``): a join to rows that
  are not unique would multiply the rows of the source;
- its dimensions are the columns of the table, and the key and coded columns (with an
  ``enum`` or ``allowedValues``) of the joined tables, named ``<join>_<column>``, other
  than the columns of the join itself. Each has the description of its column as
  ``comment``, its ``businessName`` as ``display_name`` and its ``synonyms``. A column
  whose ``enum`` has labels has a second dimension, ``<name>_label``, its codes replaced
  by their labels: it takes the business name and the synonyms of the column, whose code
  dimension is displayed as ``<business name> (code)``. The columns of the
  ``MetricViewSettings.excluded_classifications`` are left out;
- its measures are those of the contract.

``build_metric_views`` returns the metric views of a data product, with the tables named
in the environment of the service context, and their YAML (version 1.1).
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import yaml

from cds.config import MetricViewSettings, resolved
from cds.context import ServiceContext
from cds.data_context import DataProduct, Table
from cds.errors import ContractError
from cds.genie import Join, joins
from cds.odcs import SchemaProperty
from cds.semantics import Measure, metric_view_name, synonyms, table_measures
from cds.services.checks import ALLOWED_VALUES
from cds.sql import quote_identifier, string_literal

# The YAML version, a number as in the specification: 1.1 adds the agent metadata.
VERSION = 1.1
LABEL_SUFFIX = "_label"
# Limits of the metric views.
MAX_SYNONYMS = 10
MAX_DISPLAY_NAME_LENGTH = 255
# Delimiter of the YAML in the statement creating a metric view.
DELIMITER = "$$"


@dataclass(frozen=True)
class MetricView:
    """The metric view of a table.

    Attributes:
        table: Its source table.
        name: Its name, in the schema of the table.
        definition: Its definition, as the metric view YAML describes it.
        measures: The measures of the contract.
    """

    table: Table
    name: str
    definition: Mapping[str, Any]
    measures: tuple[Measure, ...] = field(default=())

    @property
    def yaml(self) -> str:
        """The YAML of the definition, in the order of the metric view specification."""
        return yaml.safe_dump(dict(self.definition), sort_keys=False, allow_unicode=True, width=10_000)

    @property
    def digest(self) -> str:
        """A digest of the YAML: Unity Catalog stores the YAML rewritten, which cannot be compared."""
        return hashlib.sha256(self.yaml.encode("utf-8")).hexdigest()[:32]


def _excluded(table: Table, prop: SchemaProperty) -> bool:
    excluded = resolved(table.settings(MetricViewSettings).excluded_classifications)
    return prop.classification is not None and prop.classification in excluded


def _synonyms(prop: SchemaProperty, where: str) -> list[str]:
    found = list(synonyms(prop.custom_properties, where))
    if len(found) > MAX_SYNONYMS:
        raise ContractError(f"{where}: a field of a metric view has at most {MAX_SYNONYMS} synonyms.")
    return found


def _metadata(
    *, comment: str | None, display_name: str | None, synonym_list: Sequence[str], where: str
) -> dict[str, Any]:
    """The optional keys of a field or a measure: those that are set."""
    if display_name is not None and len(display_name) > MAX_DISPLAY_NAME_LENGTH:
        raise ContractError(f"{where}: a display name has at most {MAX_DISPLAY_NAME_LENGTH} characters.")
    metadata: dict[str, Any] = {}
    if comment:
        metadata["comment"] = " ".join(comment.split())
    if display_name:
        metadata["display_name"] = display_name
    if synonym_list:
        metadata["synonyms"] = list(synonym_list)
    return metadata


def _literal(value: object) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return string_literal(str(value))


def _fields(table: Table, prop: SchemaProperty, alias: str, name: str) -> list[dict[str, Any]]:
    """The dimensions of a column: its codes, and their labels if its ``enum`` has some."""
    where = f"{table.qualified_name}.{prop.column}"
    expr = f"{alias}.{quote_identifier(prop.column)}"
    business_name = (prop.business_name or "").strip() or None
    synonym_list = _synonyms(prop, where)
    if not any(value.label for value in prop.enum):
        metadata = _metadata(
            comment=prop.description, display_name=business_name, synonym_list=synonym_list, where=where
        )
        return [{"name": name, "expr": expr, **metadata}]
    cases = " ".join(
        f"WHEN {_literal(value.value)} THEN {string_literal(value.label or str(value.value))}" for value in prop.enum
    )
    label = f"CASE {expr} {cases} ELSE CAST({expr} AS STRING) END"
    code_metadata = _metadata(
        comment=prop.description,
        display_name=f"{business_name} (code)" if business_name else None,
        synonym_list=(),
        where=where,
    )
    label_metadata = _metadata(
        comment=prop.description, display_name=business_name, synonym_list=synonym_list, where=where
    )
    return [
        {"name": name, "expr": expr, **code_metadata},
        {"name": f"{name}{LABEL_SUFFIX}", "expr": label, **label_metadata},
    ]


def _coded(prop: SchemaProperty) -> bool:
    return bool(prop.enum) or ALLOWED_VALUES in prop.custom_properties


def _join(join: Join, context: ServiceContext) -> dict[str, Any]:
    condition = " AND ".join(
        f"source.{quote_identifier(column)} = {join.alias}.{quote_identifier(target_column)}"
        for column, target_column in zip(join.columns, join.target_columns, strict=True)
    )
    return {"name": join.alias, "source": context.full_table_name(join.target), "on": condition}


def _measure(measure: Measure, where: str) -> dict[str, Any]:
    if len(measure.synonyms) > MAX_SYNONYMS:
        raise ContractError(f"{where}: a measure of a metric view has at most {MAX_SYNONYMS} synonyms.")
    metadata = _metadata(
        comment=measure.description, display_name=measure.business_name, synonym_list=measure.synonyms, where=where
    )
    return {
        "name": measure.name,
        "expr": " ".join(measure.expr.split()),
        **metadata,
        **({"format": dict(measure.format)} if measure.format else {}),
    }


def build_metric_view(table: Table, context: ServiceContext, found: Sequence[Join] | None = None) -> MetricView | None:
    """The metric view of a table, ``None`` if it has none (see ``cds.semantics.metric_view_name``).

    Args:
        table: The table.
        context: The service context, naming the tables in its environment.
        found: The joins of the data product of the table (see ``cds.genie.joins``);
            computed if not given.

    Raises:
        ContractError: If a measure is invalid, two fields or measures have the same name,
            or a limit of the metric views is exceeded.
    """
    name = metric_view_name(table)
    if name is None:
        return None
    declared = table_measures(table)
    context.full_table_name(table, name)
    table_joins = [
        join
        for join in (found if found is not None else joins(table.data_product, context))
        if join.table is table and join.cardinality.endswith("_TO_ONE")
    ]
    fields = [
        item
        for prop in table.columns
        if not _excluded(table, prop)
        for item in _fields(table, prop, "source", prop.column)
    ]
    for join in table_joins:
        for prop in join.target.columns:
            column = prop.column
            if column in join.target_columns or _excluded(join.target, prop):
                continue
            if column in join.target.primary_key or _coded(prop):
                fields += _fields(join.target, prop, join.alias, f"{join.alias}_{column}")
    definition: dict[str, Any] = {"version": VERSION, "source": context.full_table_name(table)}
    if table.description:
        definition["comment"] = " ".join(table.description.split())
    if table_joins:
        definition["joins"] = [_join(join, context) for join in table_joins]
    definition["fields"] = fields
    definition["measures"] = [
        _measure(measure, f"{table.qualified_name}: measure {measure.name!r}") for measure in declared
    ]
    seen: set[str] = set()
    for item in [*fields, *definition["measures"]]:
        key = item["name"].casefold()
        if key in seen:
            raise ContractError(
                f"{table.qualified_name}: the metric view {name} has two fields or measures named {item['name']!r}."
            )
        seen.add(key)
    view = MetricView(table, name, definition, declared)
    if DELIMITER in view.yaml:
        raise ContractError(f"{table.qualified_name}: the metric view {name} cannot contain {DELIMITER}.")
    return view


def build_metric_views(data_product: DataProduct, context: ServiceContext) -> list[MetricView]:
    """The metric views of the tables of a data product that declare measures, if they are enabled.

    Raises:
        ContractError: If a metric view is invalid, or two tables have the same metric view.
    """
    tables = [table for table in data_product.tables if table.settings(MetricViewSettings).enabled]
    if not tables:
        return []
    found = joins(data_product, context)
    views = [view for table in tables if (view := build_metric_view(table, context, found)) is not None]
    names: dict[str, MetricView] = {}
    table_names = {table.name.lower() for table in data_product.tables}
    for view in views:
        other = names.setdefault(view.name.lower(), view)
        if other is not view:
            raise ContractError(
                f"{data_product.qualified_name}: the tables {other.table.name} and {view.table.name} have the same "
                f"metric view {view.name}."
            )
        if view.name.lower() in table_names:
            raise ContractError(f"{view.table.qualified_name}: its metric view {view.name} is the name of a table.")
    return sorted(views, key=lambda view: view.name)
