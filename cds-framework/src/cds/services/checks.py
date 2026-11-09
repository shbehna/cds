"""Inference of DQX quality checks from the data contracts.

The checks are DQX metadata dictionaries, applied with
``DQEngine.apply_checks_by_metadata``. They are inferred from the contract of a table:

- ``required`` properties and primary key columns: ``is_not_null``, except for a key
  column declared ``required: false``;
- the primary key, and ``unique`` properties: ``is_unique``; nulls of a key with
  nullable columns are compared as values (``nulls_distinct`` false);
- ``enum`` values: ``is_in_list``;
- ``logicalTypeOptions``: ``regex_match`` for ``pattern``, range checks for
  ``minimum``, ``maximum`` and their exclusive variants, SQL checks for ``minLength``,
  ``maxLength``, ``multipleOf``, ``minItems``, ``maxItems``, ``uniqueItems``,
  ``required`` (STRUCT and MAP columns) and ``minProperties``, ``maxProperties`` (MAP
  columns), and DQX format checks for the ``format`` of string values;
- ``relationships`` of type ``foreignKey``: ``foreign_key``, the referenced table being
  resolved in the data context;
- ``quality`` rules: ODCS library metrics that DQX can check row by row (``nullValues``,
  ``missingValues``, ``invalidValues`` and ``duplicateValues`` that must be 0) and
  ``rowCount`` thresholds, and ``custom`` rules of the ``dqx`` engine, whose
  implementation is a DQX check.

The ``quality_checks`` of the contract configuration are added to these checks. The
rules and options no check implements are reported in ``CheckSet.unsupported``.
"""

from __future__ import annotations

import copy
import logging
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import yaml

from cds.config import QualitySettings
from cds.errors import ConfigurationError, ContractError
from cds.injection import ContractInjection
from cds.odcs import FOREIGN_KEY, QualityRule, Relationship, SchemaProperty
from cds.sql import quote_identifier, quote_name, string_literal

if TYPE_CHECKING:
    from cds.context import ServiceContext
    from cds.data_context import Table

logger = logging.getLogger(__name__)

DQX_ENGINE = "dqx"

# DQX criticality of an ODCS quality rule, by severity.
SEVERITIES: Mapping[str, str] = {
    "error": "error",
    "critical": "error",
    "warning": "warn",
    "warn": "warn",
    "info": "warn",
}


# logicalTypeOptions implemented by checks. minProperties and maxProperties also are, on
# MAP columns; the other options are reported as not checked.
CHECKED_OPTIONS = frozenset(
    {
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "multipleOf",
        "minItems",
        "maxItems",
        "uniqueItems",
        "required",
        "format",
    }
)

# DQX check of each format of string values (the format of dates is checked by parsing them).
STRING_FORMATS: Mapping[str, str] = {
    "email": "is_valid_email",
    "uri": "is_valid_url",
    "url": "is_valid_url",
    "uuid": "is_valid_uuid",
    "ipv4": "is_valid_ipv4_address",
    "ipv6": "is_valid_ipv6_address",
    "json": "is_valid_json",
}


@dataclass
class CheckSet:
    """The DQX checks of a table, and the contract rules DQX cannot check."""

    checks: list[dict[str, Any]] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)


def make_check(name: str, criticality: str, function: str, **arguments: object) -> dict[str, Any]:
    """Returns a DQX check, in its metadata representation."""
    return {"name": name, "criticality": criticality, "check": {"function": function, "arguments": arguments}}


def literal_values(values: Iterable[object]) -> list[object]:
    """Returns values for the ``allowed`` (or ``forbidden``) list of a DQX check.

    DQX reads a string of these lists as a SQL expression: ``"open"`` is the column
    ``open``, and ``"1"`` the number 1. Strings are therefore returned as SQL string
    literals; other values (numbers, booleans) are returned unchanged.
    """
    return [string_literal(value) if isinstance(value, str) else value for value in values]


def build_checks(table: Table, context: ServiceContext, *, include_configured: bool = True) -> CheckSet:
    """Returns the checks of a table: inferred from its contract, then configured.

    Args:
        table: The table.
        context: The service context, used to resolve the referenced tables.
        include_configured: Whether to add the ``quality_checks`` of the contract configuration.

    Raises:
        ContractError: If a rule of the contract is invalid.
        ConfigurationError: If the configured checks are invalid.
    """
    check_set = CheckSet()
    _ContractChecks(table, context, check_set).build()
    if include_configured:
        check_set.checks.extend(configured_checks(table, context))
    return check_set


def configured_checks(table: Table, context: ServiceContext) -> list[dict[str, Any]]:
    """Returns the ``quality_checks`` of the configuration of the contract of a table.

    They may be a list of checks, a mapping of table name to list, or a function
    returning either, called with injected parameters.
    """
    configured = getattr(table.contract.config, "quality_checks", None)
    if callable(configured):
        injection = ContractInjection(context, table=table, contract=table.contract, data_product=table.data_product)
        configured = injection.call(configured)
    if configured is None:
        return []
    if isinstance(configured, Mapping):
        # A single check, or the checks of each table of the contract.
        configured = [configured] if "check" in configured else configured.get(table.name, [])
    elif len(table.contract.tables) > 1:
        raise ConfigurationError(
            f"{table.contract.config_path}: the contract describes several tables, so quality_checks "
            "must be a mapping of table name to checks."
        )
    if isinstance(configured, (str, bytes)) or not isinstance(configured, Sequence):
        raise ConfigurationError(f"{table.contract.config_path}: quality_checks must be a list of DQX checks.")
    checks = []
    for item in configured:
        if not isinstance(item, Mapping) or "check" not in item:
            raise ConfigurationError(
                f"{table.contract.config_path}: each quality check must be a DQX check mapping with a "
                f"'check' entry, got {item!r}."
            )
        checks.append(copy.deepcopy(dict(item)))
    return checks


class _ContractChecks:
    def __init__(self, table: Table, context: ServiceContext, check_set: CheckSet) -> None:
        self.table = table
        self.context = context
        self.check_set = check_set
        self.criticality = table.settings(QualitySettings).criticality or {}

    def build(self) -> None:
        keys = self.table.primary_key
        for prop in self.table.columns:
            self._property_checks(prop, keys)
        if keys:
            arguments: dict[str, object] = {"columns": list(keys)}
            if any(prop.required is False for prop in self.table.columns if prop.column in keys):
                # Two rows with the same key, nulls included, are duplicates: DQX ignores them by default.
                arguments["nulls_distinct"] = False
            self._add("primary_key_is_not_unique", "primary_key", "is_unique", **arguments)
        for relationship in self.table.schema_object.relationships:
            self._foreign_key(relationship, None)
        for rule in self.table.schema_object.quality:
            self._quality_rule(rule, None)

    def _add(self, name: str, kind: str, function: str, **arguments: object) -> None:
        self.check_set.checks.append(make_check(name, self.criticality.get(kind, "error"), function, **arguments))

    def _property_checks(self, prop: SchemaProperty, keys: tuple[str, ...]) -> None:
        column = prop.column
        # A column of the primary key is not null, unless the contract states required: false.
        if prop.required or (column in keys and prop.required is None):
            self._add(f"{column}_is_null", "required" if prop.required else "primary_key", "is_not_null", column=column)
        if prop.unique and keys != (column,):
            self._add(f"{column}_is_not_unique", "unique", "is_unique", columns=[column])
        if prop.enum:
            allowed = literal_values(value.value for value in prop.enum)
            self._add(f"{column}_is_not_in_enum", "enum", "is_in_list", column=column, allowed=allowed)
        self._logical_type_options(prop)
        for relationship in prop.relationships:
            self._foreign_key(relationship, prop)
        for rule in prop.quality:
            self._quality_rule(rule, prop)

    def _logical_type_options(self, prop: SchemaProperty) -> None:
        options = {key: value for key, value in prop.logical_type_options.items() if value is not None}
        column = prop.column
        quoted = quote_identifier(column)
        if options.get("pattern"):
            self._add(
                f"{column}_does_not_match_pattern",
                "pattern",
                "regex_match",
                column=column,
                regex=str(options["pattern"]),
            )
        conditions: list[tuple[str, object]] = []
        for key, operator, exclusive_key, strict_operator in (
            ("minimum", ">=", "exclusiveMinimum", ">"),
            ("maximum", "<=", "exclusiveMaximum", "<"),
        ):
            limit, exclusive = options.get(key), options.get(exclusive_key)
            if _is_number(exclusive):
                # JSON Schema style: the exclusive bound is the value itself.
                limit, exclusive = exclusive, True
            if limit is not None:
                conditions.append((strict_operator if exclusive is True else operator, limit))
        if conditions:
            self._range(column, conditions)
        self._bounds(prop, options, "minLength", "maxLength", f"length({quoted})", "has_invalid_length", "length")
        if "multipleOf" in options:
            divisor = self._number(prop, options, "multipleOf")
            self._expression(
                prop, "range", "is_not_multiple", f"{quoted} % {divisor!r} = 0", f"is not a multiple of {divisor!r}"
            )
        self._bounds(prop, options, "minItems", "maxItems", f"size({quoted})", "has_invalid_item_count", "item count")
        if options.get("uniqueItems") is True:
            self._expression(
                prop,
                "unique",
                "has_duplicate_items",
                f"size(array_distinct({quoted})) = size({quoted})",
                "has duplicate items",
            )
        is_map = prop.data_type.strip().upper().startswith("MAP")
        if is_map:
            self._bounds(
                prop, options, "minProperties", "maxProperties", f"size({quoted})", "has_invalid_key_count", "key count"
            )
        if options.get("required"):
            self._required_properties(prop, options["required"], is_map)
        if options.get("format"):
            self._format(prop, str(options["format"]))
        checked = CHECKED_OPTIONS | ({"minProperties", "maxProperties"} if is_map else set())
        for key in sorted(set(options) - checked):
            self._not_checked(prop, f"logicalTypeOptions {key}: {options[key]!r}")

    def _bounds(
        self,
        prop: SchemaProperty,
        options: Mapping[str, object],
        minimum: str,
        maximum: str,
        measure: str,
        suffix: str,
        label: str,
    ) -> None:
        """Adds a check of the bounds of a measure of the values, such as their length."""
        bounds = [
            f"{measure} {operator} {self._integer(prop, options, key)}"
            for key, operator in ((minimum, ">="), (maximum, "<="))
            if key in options
        ]
        if bounds:
            self._expression(prop, "range", suffix, " AND ".join(bounds), f"has an invalid {label}")

    def _required_properties(self, prop: SchemaProperty, required: object, is_map: bool) -> None:
        if isinstance(required, str) or not isinstance(required, Sequence):
            raise ContractError(f"{self._where(prop)}: logicalTypeOptions required must be a list of property names.")
        quoted = quote_identifier(prop.column)
        if is_map:
            conditions = [f"map_contains_key({quoted}, {string_literal(str(name))})" for name in required]
        elif prop.data_type.strip().upper().startswith("STRUCT"):
            conditions = [f"{quoted}.{quote_identifier(str(name))} IS NOT NULL" for name in required]
        else:
            self._not_checked(prop, f"logicalTypeOptions required: {list(required)!r} on a {prop.data_type} column")
            return
        self._expression(
            prop, "required", "misses_required_properties", " AND ".join(conditions), "misses required properties"
        )

    def _format(self, prop: SchemaProperty, value: str) -> None:
        """Checks the ``format`` of string values: a known string format, or the format of dates."""
        column = prop.column
        if not prop.data_type.strip().upper().startswith(("STRING", "VARCHAR", "CHAR")):
            # The values are typed: their format is that of their source representation.
            self._not_checked(prop, f"logicalTypeOptions format: {value!r} on a {prop.data_type} column")
            return
        logical_type = (prop.logical_type or "").lower()
        if logical_type == "date":
            self._add(f"{column}_has_invalid_format", "format", "is_valid_date", column=column, date_format=value)
        elif logical_type == "timestamp":
            self._add(
                f"{column}_has_invalid_format", "format", "is_valid_timestamp", column=column, timestamp_format=value
            )
        elif value.lower() in STRING_FORMATS:
            self._add(f"{column}_has_invalid_format", "format", STRING_FORMATS[value.lower()], column=column)
        else:
            self._not_checked(prop, f"logicalTypeOptions format: {value!r}")

    def _expression(self, prop: SchemaProperty, kind: str, suffix: str, expression: str, message: str) -> None:
        """Adds a SQL check of the values of a column; null values pass."""
        quoted = quote_identifier(prop.column)
        self._add(
            f"{prop.column}_{suffix}",
            kind,
            "sql_expression",
            expression=f"{quoted} IS NULL OR ({expression})",
            msg=f"{prop.column} {message}",
            columns=[prop.column],
        )

    def _not_checked(self, prop: SchemaProperty | None, note: str) -> None:
        """Records a rule of the contract that no check implements."""
        self.check_set.unsupported.append(f"{self._where(prop)}: {note}")

    def _number(self, prop: SchemaProperty, options: Mapping[str, object], key: str) -> int | float:
        value = options[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ContractError(f"{self._where(prop)}: logicalTypeOptions {key} must be a positive number.")
        return value

    def _integer(self, prop: SchemaProperty, options: Mapping[str, object], key: str) -> int:
        value = options[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ContractError(f"{self._where(prop)}: logicalTypeOptions {key} must be a non-negative integer.")
        return value

    def _range(self, column: str, conditions: list[tuple[str, object]]) -> None:
        numeric = all(isinstance(limit, (int, float)) and not isinstance(limit, bool) for _, limit in conditions)
        inclusive = all(operator in (">=", "<=") for operator, _ in conditions)
        name = f"{column}_is_out_of_range"
        if numeric and inclusive:
            limits = {operator: limit for operator, limit in conditions}
            if len(limits) == 2:
                self._add(name, "range", "is_in_range", column=column, min_limit=limits[">="], max_limit=limits["<="])
            elif ">=" in limits:
                self._add(name, "range", "is_not_less_than", column=column, limit=limits[">="])
            else:
                self._add(name, "range", "is_not_greater_than", column=column, limit=limits["<="])
            return
        # Exclusive bounds and non numeric limits (e.g. dates) are expressed in SQL.
        expression = " AND ".join(
            f"{quote_identifier(column)} {operator} {_sql_value(limit)}" for operator, limit in conditions
        )
        self._add(
            name, "range", "sql_expression", expression=expression, msg=f"{column} is out of range", columns=[column]
        )

    def _foreign_key(self, relationship: Relationship, prop: SchemaProperty | None) -> None:
        if relationship.type != FOREIGN_KEY:
            self._not_checked(prop, f"relationship of type {relationship.type!r}")
            return
        data_context = self.context.data_context
        sources = [prop.column] if prop is not None else [self._source_column(ref) for ref in relationship.source]
        targets = [data_context.resolve_reference(ref, self.table) for ref in relationship.target]
        referenced = {table.qualified_name for table, _ in targets}
        if len(referenced) != 1:
            raise ContractError(f"{self._where(prop)}: a relationship must reference a single table.")
        ref_table = targets[0][0]
        self._add(
            f"{'_'.join(sources)}_not_in_{ref_table.name}",
            "foreign_key",
            "foreign_key",
            columns=sources,
            ref_columns=[column for _, column in targets],
            ref_table=quote_name(self.context.full_table_name(ref_table)),
        )

    def _source_column(self, reference: str) -> str:
        if "." not in reference:
            prop = self.table.get_column(reference)
            if prop is None:
                raise ContractError(f"{self.table.qualified_name}: the relationship source {reference!r} is unknown.")
            return prop.column
        table, column = self.context.data_context.resolve_reference(reference, self.table)
        if table is not self.table:
            raise ContractError(
                f"{self.table.qualified_name}: the relationship source {reference!r} must be a property of the table."
            )
        return column

    def _quality_rule(self, rule: QualityRule, prop: SchemaProperty | None) -> None:
        if rule.type == "text":
            return
        if rule.type == "custom":
            if (rule.engine or "").lower() == DQX_ENGINE:
                self.check_set.checks.extend(self._dqx_rule(rule, prop))
            else:
                self._not_checked(prop, f"custom rule for the engine {rule.engine!r}")
            return
        if rule.type == "library":
            checks = self._library_rule(rule, prop)
            if checks:
                self.check_set.checks.extend(checks)
            else:
                self._not_checked(prop, f"library rule {rule.metric!r} with {dict(rule.thresholds) or 'no threshold'}")
            return
        self._not_checked(prop, f"{rule.type} rule {rule.name or ''}".rstrip())

    def _library_rule(self, rule: QualityRule, prop: SchemaProperty | None) -> list[dict[str, Any]]:
        metric = rule.metric or ""
        criticality = _criticality(rule)
        if metric == "rowCount":
            return [] if prop is not None else _row_count_checks(rule, criticality)
        if not _must_be_zero(rule.thresholds):
            return []
        column = prop.column if prop is not None else None
        if metric == "nullValues" and column:
            return [make_check(rule.name or f"{column}_is_null", criticality, "is_not_null", column=column)]
        if metric == "missingValues" and column:
            return [
                make_check(rule.name or f"{column}_is_missing", criticality, "is_not_null_and_not_empty", column=column)
            ]
        if metric == "invalidValues" and column:
            checks = []
            if rule.arguments.get("validValues"):
                checks.append(
                    make_check(
                        rule.name or f"{column}_is_invalid",
                        criticality,
                        "is_in_list",
                        column=column,
                        allowed=literal_values(rule.arguments["validValues"]),
                    )
                )
            if rule.arguments.get("pattern"):
                checks.append(
                    make_check(
                        f"{rule.name}_pattern"
                        if rule.name and checks
                        else rule.name or f"{column}_does_not_match_pattern",
                        criticality,
                        "regex_match",
                        column=column,
                        regex=str(rule.arguments["pattern"]),
                    )
                )
            return checks
        if metric == "duplicateValues":
            columns = (
                [column] if column else [self._source_column(name) for name in rule.arguments.get("properties") or []]
            )
            if columns:
                return [
                    make_check(
                        rule.name or f"{'_'.join(columns)}_is_not_unique", criticality, "is_unique", columns=columns
                    )
                ]
        return []

    def _dqx_rule(self, rule: QualityRule, prop: SchemaProperty | None) -> list[dict[str, Any]]:
        implementation = rule.implementation
        if isinstance(implementation, str):
            try:
                implementation = yaml.safe_load(implementation)
            except yaml.YAMLError as exc:
                raise ContractError(f"{self._where(prop)}: invalid DQX implementation: {exc}") from exc
        items = implementation if isinstance(implementation, list) else [implementation]
        checks = []
        for item in items:
            if not isinstance(item, Mapping) or not isinstance(item.get("check"), Mapping):
                raise ContractError(f"{self._where(prop)}: a DQX implementation must be a check with a 'check' entry.")
            check = copy.deepcopy(dict(item))
            check.setdefault("criticality", _criticality(rule))
            if rule.name and len(items) == 1:
                check.setdefault("name", rule.name)
            arguments = check["check"].setdefault("arguments", {})
            if prop is not None and "column" not in arguments and "columns" not in arguments:
                arguments["column"] = prop.column
            checks.append(check)
        return checks

    def _where(self, prop: SchemaProperty | None) -> str:
        return f"{self.table.qualified_name}.{prop.column}" if prop is not None else self.table.qualified_name


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _criticality(rule: QualityRule) -> str:
    if not rule.severity:
        return "error"
    return SEVERITIES.get(rule.severity.lower(), "error")


def _must_be_zero(thresholds: Mapping[str, Any]) -> bool:
    return (
        thresholds.get("mustBe") == 0
        or thresholds.get("mustBeLessOrEqualTo") == 0
        or thresholds.get("mustBeLessThan") == 1
    )


def _row_count_checks(rule: QualityRule, criticality: str) -> list[dict[str, Any]]:
    thresholds = rule.thresholds
    limits: list[tuple[str, str, Any]] = []
    if "mustBe" in thresholds:
        limits.append(("equal", "is_aggr_equal", thresholds["mustBe"]))
    if "mustNotBe" in thresholds:
        limits.append(("not_equal", "is_aggr_not_equal", thresholds["mustNotBe"]))
    if "mustBeGreaterThan" in thresholds:
        limits.append(("minimum", "is_aggr_not_less_than", math.floor(thresholds["mustBeGreaterThan"]) + 1))
    if "mustBeGreaterOrEqualTo" in thresholds:
        limits.append(("minimum", "is_aggr_not_less_than", math.ceil(thresholds["mustBeGreaterOrEqualTo"])))
    if "mustBeLessThan" in thresholds:
        limits.append(("maximum", "is_aggr_not_greater_than", math.ceil(thresholds["mustBeLessThan"]) - 1))
    if "mustBeLessOrEqualTo" in thresholds:
        limits.append(("maximum", "is_aggr_not_greater_than", math.floor(thresholds["mustBeLessOrEqualTo"])))
    if "mustBeBetween" in thresholds:
        low, high = thresholds["mustBeBetween"]
        limits.append(("minimum", "is_aggr_not_less_than", math.ceil(low)))
        limits.append(("maximum", "is_aggr_not_greater_than", math.floor(high)))
    base = rule.name or "row_count"
    return [
        make_check(
            f"{base}_{suffix}" if len(limits) > 1 or not rule.name else base,
            criticality,
            function,
            column="*",
            aggr_type="count",
            limit=limit,
        )
        for suffix, function, limit in limits
    ]


def _sql_value(value: object) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return string_literal(str(value))


def validate_with_dqx(checks: list[dict[str, Any]]) -> list[str]:
    """Validates checks with DQX and returns the problems found.

    Raises:
        ConfigurationError: If DQX is not installed.
    """
    try:
        from databricks.labs.dqx.engine import DQEngine
    except ImportError as exc:
        raise ConfigurationError("DQX is required by the quality service: install cds-framework[dqx].") from exc
    return list(DQEngine.validate_checks(checks).errors)
