"""Service level agreements (SLA) of the data contracts.

The ``slaProperties`` of a contract state the service levels of its tables. ODCS names
the standard properties after the Data QoS periodic table, each with an abbreviation:
``latency`` (``ly``), ``frequency`` (``fy``), ``errorRate`` (``er``), ``retention``
(``re``)... The SCD service applies the ``retention`` of a table to its history. The
other properties are read by the deployments, for example to schedule the loads
(``frequency``) or to alert on stale data (``latency``) and error rates (``errorRate``).
``cds validate`` checks them all.

A property applies to the tables of its ``element``: a table, or a column of a table
(``table.column``), several separated by commas; in a contract with a single table, a
column may be named alone. Without element, it applies to every table of the contract. A
property whose element designates a table takes precedence over the one without element.

The durations (``latency``, ``retention``, ``frequency`` and the ``timeTo...``
properties) are positive integers with a unit: ``h`` (hours), ``d`` (days), ``w``
(weeks), ``mo`` (months) or ``y`` (years), or their long forms (``days``...). The
``errorRate`` and the ``availability`` are percentages: a number from 0 to 100, with the
unit ``percent`` or ``%``, the default.

``scheduler: cron`` and ``schedule`` give when a property is run or checked, as a cron
expression of five fields (minute, hour, day of month, month, day of week).
``quartz_cron`` converts it to the Quartz syntax of the Databricks jobs and alerts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import unquote

from cds.errors import ContractError
from cds.odcs import DataContract, SchemaObject, SchemaProperty, SlaProperty, SupportChannel

if TYPE_CHECKING:
    from cds.data_context import Table

# The standard properties of ODCS, with their abbreviations.
PROPERTIES: Mapping[str, str | None] = {
    "availability": "av",
    "throughput": "th",
    "errorRate": "er",
    "generalAvailability": "ga",
    "endOfSupport": "es",
    "endOfLife": "el",
    "retention": "re",
    "frequency": "fy",
    "latency": "ly",
    "timeOfAvailability": None,
    "timeToDetect": "td",
    "timeToNotify": "tn",
    "timeToRepair": "tr",
}
_NAMES = {name.lower(): name for name in PROPERTIES} | {
    abbreviation: name for name, abbreviation in PROPERTIES.items() if abbreviation
}

DURATION_PROPERTIES = frozenset({"latency", "retention", "frequency", "timeToDetect", "timeToNotify", "timeToRepair"})
PERCENTAGE_PROPERTIES = frozenset({"errorRate", "availability"})
# The properties whose schedule the deployments apply: it must be a cron expression.
SCHEDULED_PROPERTIES = frozenset({"frequency", "latency", "errorRate"})

# Units of the durations: the unit of the SQL interval, and the number of them in one.
_DURATION_UNITS: Mapping[str, tuple[str, int]] = {
    **dict.fromkeys(("h", "hr", "hour", "hours"), ("HOUR", 1)),
    **dict.fromkeys(("d", "day", "days"), ("DAY", 1)),
    **dict.fromkeys(("w", "week", "weeks"), ("DAY", 7)),
    **dict.fromkeys(("mo", "month", "months"), ("MONTH", 1)),
    **dict.fromkeys(("y", "yr", "year", "years"), ("YEAR", 1)),
}
_PERCENT_UNITS = ("percent", "%")

_MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
_DAYS = ("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT")
# The fields of a cron expression: name, lowest and highest values, names of the values.
_CRON_FIELDS: Sequence[tuple[str, int, int, Sequence[str]]] = (
    ("minute", 0, 59, ()),
    ("hour", 0, 23, ()),
    ("day of month", 1, 31, ()),
    ("month", 1, 12, _MONTHS),
    # 0 and 7 are Sunday.
    ("day of week", 0, 7, _DAYS),
)


@dataclass(frozen=True)
class Duration:
    """A duration of a service level, such as a latency.

    Attributes:
        amount: Number of units.
        unit: Unit of the SQL interval: ``HOUR``, ``DAY``, ``MONTH`` or ``YEAR``.
    """

    amount: int
    unit: str

    def sql(self) -> str:
        """The SQL interval literal of the duration, such as ``INTERVAL '4' DAY``."""
        return f"INTERVAL '{self.amount}' {self.unit}"

    def __str__(self) -> str:
        return f"{self.amount} {self.unit.lower()}{'' if self.amount == 1 else 's'}"


def canonical_property(name: str) -> str | None:
    """Returns the standard name of a property, given its name or abbreviation in any case."""
    return _NAMES.get(name.strip().lower())


def duration(prop: SlaProperty) -> Duration:
    """Returns the duration of a property, such as a latency or a retention.

    Raises:
        ContractError: If its value is not a positive integer, or its unit not a duration.
    """
    value = _number(prop.value)
    if not isinstance(value, int) or value <= 0:
        raise ContractError(f"{prop.property}: the value must be a positive integer, got {prop.value!r}")
    unit = _DURATION_UNITS.get((prop.unit or "").strip().lower())
    if unit is None:
        raise ContractError(
            f"{prop.property}: the unit must be a duration, h, d, w, mo or y (or hours, days...), got {prop.unit!r}"
        )
    sql_unit, factor = unit
    return Duration(value * factor, sql_unit)


def percentage(prop: SlaProperty) -> float:
    """Returns the percentage of a property, such as an error rate.

    Raises:
        ContractError: If its value is not a number from 0 to 100, or its unit not a percentage.
    """
    value = _number(prop.value)
    if value is None or not 0 <= value <= 100:
        raise ContractError(f"{prop.property}: the value must be a percentage, from 0 to 100, got {prop.value!r}")
    if prop.unit is not None and prop.unit.strip().lower() not in _PERCENT_UNITS:
        raise ContractError(f"{prop.property}: the unit must be percent or %, got {prop.unit!r}")
    return float(value)


def _number(value: object) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return None
    return None


def cron_problem(expression: str) -> str | None:
    """Returns why a string is not a cron expression of five fields, or ``None`` if it is one."""
    fields = expression.split()
    if len(fields) != len(_CRON_FIELDS):
        return f"the cron expression {expression!r} must have 5 fields: minute, hour, day of month, month, day of week"
    for text, (name, low, high, names) in zip(fields, _CRON_FIELDS, strict=True):
        for item in text.split(","):
            problem = _cron_item_problem(item, low, high, names)
            if problem:
                return f"the cron expression {expression!r}: {name} {text!r}: {problem}"
    return None


def _cron_item_problem(item: str, low: int, high: int, names: Sequence[str]) -> str | None:
    base, slash, step = item.partition("/")
    if slash and (not step.isdigit() or int(step) == 0):
        return f"invalid step {step!r}"
    if base == "*":
        return None
    bounds = base.split("-")
    if len(bounds) > 2:
        return f"invalid range {base!r}"
    values = [_cron_value(bound, names, low) for bound in bounds]
    for bound, number in zip(bounds, values, strict=True):
        if number is None or not low <= number <= high:
            return f"{bound!r} is not a value from {low} to {high}"
    if len(values) == 2 and values[0] is not None and values[1] is not None and values[0] > values[1]:
        return f"invalid range {base!r}"
    return None


def _cron_value(text: str, names: Sequence[str], first: int) -> int | None:
    if text.isdigit():
        return int(text)
    upper = text.upper()
    return names.index(upper) + first if upper in names else None


def quartz_cron(expression: str) -> str:
    """Converts a cron expression of five fields to the Quartz syntax of Databricks.

    Quartz adds the seconds, and marks with ``?`` the day field left free; the days of the
    week are named, as cron counts them from 0 (Sunday) and Quartz from 1:
    ``0 6 1,15 * *`` becomes ``0 0 6 1,15 * ?``.

    Raises:
        ContractError: If the expression is invalid, restricts both the day of month and
            the day of week, which Quartz cannot express, or has a step in the day of week.
    """
    problem = cron_problem(expression)
    if problem:
        raise ContractError(problem)
    minute, hour, day, month, weekday = expression.split()
    if weekday == "*":
        weekday = "?"
    elif day == "*":
        day = "?"
        weekday = _quartz_weekdays(expression, weekday)
    else:
        raise ContractError(
            f"the cron expression {expression!r} restricts both the day of month and the day of week, "
            "which the Quartz syntax of Databricks cannot express"
        )
    return " ".join(("0", minute, hour, day, month, weekday))


def _quartz_weekdays(expression: str, text: str) -> str:
    items = []
    for item in text.split(","):
        if "/" in item:
            raise ContractError(f"the cron expression {expression!r}: a step in the day of week cannot be converted")
        days = [_cron_value(bound, _DAYS, 0) for bound in item.split("-")]
        items.append("-".join(_DAYS[day % 7] for day in days if day is not None))
    return ",".join(items)


def targets(prop: SlaProperty, document: DataContract) -> list[tuple[SchemaObject, SchemaProperty | None]]:
    """Returns the tables, and columns, that a property applies to.

    Raises:
        ContractError: If its element designates no table or column of the contract.
    """
    if not prop.element:
        return [(table, None) for table in document.schema]
    found: list[tuple[SchemaObject, SchemaProperty | None]] = []
    for element in (part.strip() for part in prop.element.split(",")):
        table_name, _, column_name = element.partition(".")
        table = document.get_schema_object(table_name)
        if table is None and not column_name and len(document.schema) == 1:
            table, column_name = document.schema[0], table_name
        if table is None:
            raise ContractError(f"{prop.property}: the element {element!r} designates no table of the contract")
        column = table.get_property(column_name) if column_name else None
        if column_name and column is None:
            raise ContractError(f"{prop.property}: the element {element!r} designates no column of {table.name}")
        found.append((table, column))
    return found


def table_sla(table: Table, name: str) -> tuple[SlaProperty, str | None] | None:
    """Returns the SLA property of a table, and the column it designates, if any.

    Args:
        table: The table.
        name: Standard name of the property, such as ``retention``.

    Returns:
        The property whose element designates the table or one of its columns, else the
        property without element; ``None`` if the contract has neither.

    Raises:
        ContractError: If the element of a property of this name is invalid.
    """
    document = table.contract.document
    if document is None:
        return None
    general: tuple[SlaProperty, str | None] | None = None
    for prop in document.sla_properties:
        if canonical_property(prop.property) != name:
            continue
        if not prop.element:
            general = general or (prop, None)
            continue
        for target, column in targets(prop, document):
            if target is table.schema_object:
                return prop, column.column if column else None
    return general


def email_addresses(channel: SupportChannel) -> list[str]:
    """Returns the addresses of an email support channel: those of its ``mailto:`` url."""
    if (channel.tool or "").strip().lower() != "email" or not channel.url:
        return []
    url = channel.url.strip()
    if url.lower().startswith("mailto:"):
        url = url[len("mailto:") :].split("?", 1)[0]
    elif ":" in url:
        return []
    addresses = (unquote(address).strip() for address in url.split(","))
    return [address for address in addresses if "@" in address]


def support_emails(document: DataContract) -> list[str]:
    """Returns the addresses of the email support channels of a contract, without duplicates."""
    addresses = (address for channel in document.support for address in email_addresses(channel))
    return list(dict.fromkeys(addresses))


def sla_problems(document: DataContract) -> list[str]:
    """Returns the problems of the SLA properties and support channels of a contract, for ``cds validate``."""
    problems = []
    for index, prop in enumerate(document.sla_properties):
        where = f"slaProperties[{index}]"
        name = canonical_property(prop.property)
        if name is None:
            problems.append(
                f"{where}: unknown property {prop.property!r}, expected one of {', '.join(PROPERTIES)}, "
                "or their abbreviations"
            )
            continue
        try:
            targets(prop, document)
            if name in DURATION_PROPERTIES:
                duration(prop)
            if name in PERCENTAGE_PROPERTIES:
                percentage(prop)
            if prop.scheduler or prop.schedule:
                _check_schedule(prop, name)
        except ContractError as exc:
            problems.append(f"{where}: {exc}")
    for index, channel in enumerate(document.support):
        if (channel.tool or "").strip().lower() == "email" and not email_addresses(channel):
            problems.append(f"support[{index}]: the email channel {channel.channel!r} needs a mailto: url")
    return problems


def _check_schedule(prop: SlaProperty, name: str) -> None:
    if (prop.scheduler or "").strip().lower() != "cron":
        if name in SCHEDULED_PROPERTIES:
            raise ContractError(f"{prop.property}: CDS reads cron schedules, got the scheduler {prop.scheduler!r}")
        return
    if not prop.schedule:
        raise ContractError(f"{prop.property}: the cron scheduler needs a schedule")
    quartz_cron(prop.schedule)
