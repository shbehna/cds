"""Object model of Open Data Contract Standard (ODCS) v3 documents.

The model covers the parts of the standard the services rely on: schema objects
(tables), their properties (columns), keys, partitioning, relationships, enumerations
of allowed values, classifications, tags, data quality rules, service level agreements
(``slaProperties``) and support channels. Each element keeps its source mapping in
``raw``, so any other part of a contract remains readable.

Reference: https://bitol-io.github.io/open-data-contract-standard/latest/
"""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from cds.errors import ContractError

KIND = "DataContract"
FOREIGN_KEY = "foreignKey"

# Top-level fields identifying a contract, which a partial document cannot set.
CONTRACT_IDENTITY_FIELDS = ("apiVersion", "kind", "id", "name", "version", "schema")

# Spark SQL type of a property that declares a logicalType but no physicalType.
LOGICAL_TYPES: Mapping[str, str] = {
    "string": "STRING",
    "integer": "BIGINT",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "STRING",
}
DEFAULT_PHYSICAL_TYPE = "STRING"

# Threshold fields of library and SQL quality rules.
THRESHOLDS = (
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
)


@dataclass(frozen=True)
class EnumValue:
    """An allowed value of a property (``enum``)."""

    value: Any
    label: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class QualityRule:
    """A data quality rule (``quality``) of a schema object or a property."""

    type: str
    name: str | None = None
    description: str | None = None
    severity: str | None = None
    metric: str | None = None
    arguments: Mapping[str, Any] = field(default_factory=dict)
    thresholds: Mapping[str, Any] = field(default_factory=dict)
    engine: str | None = None
    implementation: Any = None
    query: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True)
class Relationship:
    """A relationship between properties, such as a foreign key.

    ``target`` holds the ``to`` references. ``source`` holds the ``from`` references,
    in the same order; it is empty for a relationship declared on a property, whose
    source is implicitly the property itself.
    """

    type: str
    target: tuple[str, ...]
    source: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True)
class SchemaProperty:
    """A property of a schema object: a column of a table."""

    name: str
    id: str | None = None
    physical_name: str | None = None
    logical_type: str | None = None
    physical_type: str | None = None
    description: str | None = None
    business_name: str | None = None
    # None when the contract does not state it: a column of the primary key is then required.
    required: bool | None = None
    unique: bool = False
    primary_key: bool = False
    primary_key_position: int | None = None
    partitioned: bool = False
    partition_key_position: int | None = None
    logical_type_options: Mapping[str, Any] = field(default_factory=dict)
    enum: tuple[EnumValue, ...] = ()
    # Sensitivity of the data, such as public or restricted (ODCS ``classification``).
    classification: str | None = None
    tags: tuple[str, ...] = ()
    custom_properties: Mapping[str, Any] = field(default_factory=dict)
    quality: tuple[QualityRule, ...] = ()
    relationships: tuple[Relationship, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @property
    def column(self) -> str:
        """Name of the column: the physical name, or the name."""
        return self.physical_name or self.name

    @property
    def data_type(self) -> str:
        """Spark SQL type of the column: the physical type, or the mapped logical type."""
        if self.physical_type:
            return self.physical_type
        if self.logical_type:
            return LOGICAL_TYPES[self.logical_type.lower()]
        return DEFAULT_PHYSICAL_TYPE


@dataclass(frozen=True)
class SchemaObject:
    """A schema object of a contract: a table."""

    name: str
    id: str | None = None
    physical_name: str | None = None
    physical_type: str | None = None
    description: str | None = None
    business_name: str | None = None
    properties: tuple[SchemaProperty, ...] = ()
    quality: tuple[QualityRule, ...] = ()
    relationships: tuple[Relationship, ...] = ()
    tags: tuple[str, ...] = ()
    custom_properties: Mapping[str, Any] = field(default_factory=dict)
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @property
    def table_name(self) -> str:
        """Name of the table: the physical name, or the name."""
        return self.physical_name or self.name

    @property
    def primary_key(self) -> tuple[str, ...]:
        """Columns of the primary key, ordered by ``primaryKeyPosition``.

        Columns without a position keep their order of declaration, after the others.
        """
        return _ordered_columns((prop, prop.primary_key_position) for prop in self.properties if prop.primary_key)

    @property
    def partition_columns(self) -> tuple[str, ...]:
        """Partition columns, ordered by ``partitionKeyPosition``."""
        return _ordered_columns((prop, prop.partition_key_position) for prop in self.properties if prop.partitioned)

    def get_property(self, name: str) -> SchemaProperty | None:
        """Returns the property with this name or column name (case insensitive)."""
        lowered = name.lower()
        return next(
            (prop for prop in self.properties if prop.name.lower() == lowered or prop.column.lower() == lowered),
            None,
        )


@dataclass(frozen=True)
class SlaProperty:
    """A service level agreement of a contract (``slaProperties``), see ``cds.sla``.

    Attributes:
        property: Name of the property, such as ``latency`` or ``errorRate``.
        value: Its value, such as ``4``.
        unit: Unit of the value, such as ``d`` (days).
        element: The tables or columns it applies to (``table`` or ``table.column``,
            several separated by commas); every table of the contract when not set.
        scheduler: Tool running or checking the property, such as ``cron``.
        schedule: Configuration of the scheduler, such as the cron expression ``0 20 * * *``.
    """

    property: str
    value: Any = None
    value_ext: Any = None
    unit: str | None = None
    element: str | None = None
    driver: str | None = None
    scheduler: str | None = None
    schedule: str | None = None
    description: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True)
class SupportChannel:
    """A support channel of a contract (``support``), such as an email address.

    Attributes:
        channel: Name of the channel.
        url: Its address, such as ``mailto:team@example.com``.
        tool: Its tool, such as ``email``, ``slack`` or ``teams``.
        scope: Its use, such as ``interactive``, ``announcements``, ``issues`` or ``notifications``.
    """

    channel: str
    url: str | None = None
    tool: str | None = None
    scope: str | None = None
    description: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True)
class DataContract:
    """An ODCS data contract document."""

    id: str
    version: str
    api_version: str
    status: str | None = None
    name: str | None = None
    domain: str | None = None
    data_product: str | None = None
    tenant: str | None = None
    description: Mapping[str, Any] = field(default_factory=dict)
    tags: tuple[str, ...] = ()
    schema: tuple[SchemaObject, ...] = ()
    sla_properties: tuple[SlaProperty, ...] = ()
    # Element of the SLA properties without element (deprecated since ODCS v3.1).
    sla_default_element: str | None = None
    support: tuple[SupportChannel, ...] = ()
    custom_properties: Mapping[str, Any] = field(default_factory=dict)
    source: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], source: str | Path | None = None) -> DataContract:
        """Builds a contract from its mapping representation.

        Raises:
            ContractError: If the document is not a valid ODCS v3 contract.
        """
        return _Parser(str(source) if source is not None else "<contract>").contract(data)

    @classmethod
    def from_file(cls, path: str | Path, defaults: Mapping[str, Any] | None = None) -> DataContract:
        """Reads a contract from a YAML file.

        Args:
            path: The YAML file.
            defaults: Partial document merged under the file content (see
                ``merge_documents``), typically the defaults of the data product and group.

        Raises:
            ContractError: If the file cannot be read or is not a valid ODCS v3 contract.
        """
        path = Path(path)
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise ContractError(f"Cannot read the data contract {path}: {exc}") from exc
        if not isinstance(document, Mapping):
            raise ContractError(f"{path}: a data contract must be a YAML mapping.")
        if defaults:
            document = merge_documents(defaults, document)
        return cls.from_dict(document, source=path)

    def get_schema_object(self, name: str) -> SchemaObject | None:
        """Returns the schema object with this name or table name (case insensitive)."""
        lowered = name.lower()
        return next(
            (item for item in self.schema if item.name.lower() == lowered or item.table_name.lower() == lowered),
            None,
        )


def partial_document(document: object, where: str) -> dict[str, Any]:
    """Checks a partial ODCS document and returns a copy of it.

    A partial document holds the top-level fields shared by several contracts, such as
    their ``slaProperties`` or ``support``, merged under each of them (see
    ``merge_documents``). It cannot set the fields identifying a contract
    (``CONTRACT_IDENTITY_FIELDS``).

    Args:
        document: The partial document; ``None`` is an empty document.
        where: Where the document is defined, for the error messages.

    Raises:
        ContractError: If the document is not a mapping, or sets a field identifying a contract.
    """
    if document is None:
        return {}
    if not isinstance(document, Mapping):
        raise ContractError(f"{where}: a partial data contract must be a mapping.")
    identity = [name for name in CONTRACT_IDENTITY_FIELDS if name in document]
    if identity:
        raise ContractError(
            f"{where}: a partial data contract cannot set {', '.join(identity)}: they identify each contract."
        )
    return copy.deepcopy(dict(document))


def read_partial(path: str | Path) -> dict[str, Any]:
    """Reads a partial ODCS document (see ``partial_document``) from a YAML file.

    A file without content, such as one holding only comments, is an empty document.

    Raises:
        ContractError: If the file cannot be read, or its document is invalid.
    """
    path = Path(path)
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ContractError(f"Cannot read the partial data contract {path}: {exc}") from exc
    return partial_document(document, str(path))


def merge_documents(*documents: Mapping[str, Any] | None) -> dict[str, Any]:
    """Deep-merges partial documents, later documents taking precedence.

    Mappings are merged key by key. Any other value, including lists, replaces the
    value of the previous documents.
    """
    result: dict[str, Any] = {}
    for document in documents:
        if document:
            result = _merge(result, document)
    return result


def _merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        current = merged.get(key)
        if isinstance(value, Mapping) and isinstance(current, Mapping):
            merged[key] = _merge(current, value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _ordered_columns(items: Iterable[tuple[SchemaProperty, int | None]]) -> tuple[str, ...]:
    indexed = [(position, index, prop) for index, (prop, position) in enumerate(items)]
    indexed.sort(key=lambda item: (item[0] is None, item[0] or 0, item[1]))
    return tuple(prop.column for _, _, prop in indexed)


class _Parser:
    """Converts the mapping of a contract into the object model, with precise errors."""

    def __init__(self, source: str) -> None:
        self.source = source

    def contract(self, data: object) -> DataContract:
        if not isinstance(data, Mapping):
            raise self._error("", "a data contract must be a mapping")
        kind = data.get("kind")
        if kind != KIND:
            raise self._error("kind", f"expected {KIND!r}, got {kind!r}")
        api_version = self._required_text(data, "apiVersion", "")
        if not api_version.startswith("v3."):
            raise self._error("apiVersion", f"unsupported version {api_version!r}, CDS reads ODCS v3 contracts")
        schema = tuple(
            self.schema_object(item, f"schema[{index}]") for index, item in enumerate(self._list(data, "schema", ""))
        )
        self._check_unique([item.table_name for item in schema], "schema", "table")
        description = data.get("description")
        if isinstance(description, str):
            description = {"purpose": description}
        return DataContract(
            id=self._required_text(data, "id", ""),
            version=self._required_text(data, "version", ""),
            api_version=api_version,
            status=self._text(data, "status", ""),
            name=self._text(data, "name", ""),
            domain=self._text(data, "domain", ""),
            data_product=self._text(data, "dataProduct", ""),
            tenant=self._text(data, "tenant", ""),
            description=description if isinstance(description, Mapping) else {},
            tags=self._tags(data, ""),
            schema=schema,
            sla_properties=tuple(
                self.sla_property(item, f"slaProperties[{index}]")
                for index, item in enumerate(self._list(data, "slaProperties", ""))
            ),
            sla_default_element=self._text(data, "slaDefaultElement", ""),
            support=tuple(
                self.support_channel(item, f"support[{index}]")
                for index, item in enumerate(self._list(data, "support", ""))
            ),
            custom_properties=self._custom_properties(data, ""),
            source=self.source,
            raw=data,
        )

    def sla_property(self, data: object, where: str) -> SlaProperty:
        data = self._mapping(data, where)
        return SlaProperty(
            property=self._required_text(data, "property", where),
            value=data.get("value"),
            value_ext=data.get("valueExt"),
            unit=self._text(data, "unit", where),
            element=self._text(data, "element", where),
            driver=self._text(data, "driver", where),
            scheduler=self._text(data, "scheduler", where),
            schedule=self._text(data, "schedule", where),
            description=self._text(data, "description", where),
            raw=data,
        )

    def support_channel(self, data: object, where: str) -> SupportChannel:
        data = self._mapping(data, where)
        return SupportChannel(
            channel=self._required_text(data, "channel", where),
            url=self._text(data, "url", where),
            tool=self._text(data, "tool", where),
            scope=self._text(data, "scope", where),
            description=self._text(data, "description", where),
            raw=data,
        )

    def schema_object(self, data: object, where: str) -> SchemaObject:
        data = self._mapping(data, where)
        properties = tuple(
            self.property(item, f"{where}.properties[{index}]")
            for index, item in enumerate(self._list(data, "properties", where))
        )
        self._check_unique([prop.column for prop in properties], f"{where}.properties", "column")
        return SchemaObject(
            name=self._required_text(data, "name", where),
            id=self._text(data, "id", where),
            physical_name=self._text(data, "physicalName", where),
            physical_type=self._text(data, "physicalType", where),
            description=self._text(data, "description", where),
            business_name=self._text(data, "businessName", where),
            properties=properties,
            quality=self._quality_rules(data, where),
            relationships=tuple(
                self.relationship(item, f"{where}.relationships[{index}]", property_level=False)
                for index, item in enumerate(self._list(data, "relationships", where))
            ),
            tags=self._tags(data, where),
            custom_properties=self._custom_properties(data, where),
            raw=data,
        )

    def property(self, data: object, where: str) -> SchemaProperty:
        data = self._mapping(data, where)
        name = self._required_text(data, "name", where)
        logical_type = self._text(data, "logicalType", where)
        physical_type = self._text(data, "physicalType", where)
        if not physical_type and logical_type and logical_type.lower() not in LOGICAL_TYPES:
            raise self._error(
                where,
                f"property {name!r} has the logicalType {logical_type!r}, which needs an explicit "
                "physicalType such as STRUCT<...> or ARRAY<...>",
            )
        options = data.get("logicalTypeOptions") or {}
        if not isinstance(options, Mapping):
            raise self._error(f"{where}.logicalTypeOptions", "must be a mapping")
        return SchemaProperty(
            name=name,
            id=self._text(data, "id", where),
            physical_name=self._text(data, "physicalName", where),
            logical_type=logical_type,
            physical_type=physical_type,
            description=self._text(data, "description", where),
            business_name=self._text(data, "businessName", where),
            required=self._optional_bool(data, "required", where),
            unique=self._bool(data, "unique", where),
            primary_key=self._bool(data, "primaryKey", where),
            primary_key_position=self._position(data, "primaryKeyPosition", where),
            partitioned=self._bool(data, "partitioned", where),
            partition_key_position=self._position(data, "partitionKeyPosition", where),
            logical_type_options=dict(options),
            enum=tuple(
                self.enum_value(item, f"{where}.enum[{index}]")
                for index, item in enumerate(self._list(data, "enum", where))
            ),
            classification=self._text(data, "classification", where),
            tags=self._tags(data, where),
            custom_properties=self._custom_properties(data, where),
            quality=self._quality_rules(data, where),
            relationships=tuple(
                self.relationship(item, f"{where}.relationships[{index}]", property_level=True)
                for index, item in enumerate(self._list(data, "relationships", where))
            ),
            raw=data,
        )

    def enum_value(self, data: object, where: str) -> EnumValue:
        if not isinstance(data, Mapping):
            # A plain list of values is accepted as a shorthand.
            return EnumValue(value=data)
        if "value" not in data:
            raise self._error(where, "an enum value requires 'value'")
        return EnumValue(
            value=data["value"],
            label=self._text(data, "label", where),
            description=self._text(data, "description", where),
        )

    def quality(self, data: object, where: str) -> QualityRule:
        data = self._mapping(data, where)
        arguments = data.get("arguments") or {}
        if not isinstance(arguments, Mapping):
            raise self._error(f"{where}.arguments", "must be a mapping")
        return QualityRule(
            # "library" is the default type of a quality rule in ODCS.
            type=(self._text(data, "type", where) or "library").lower(),
            name=self._text(data, "name", where),
            description=self._text(data, "description", where),
            severity=self._text(data, "severity", where),
            # "rule" is the ODCS v3.0 name of "metric".
            metric=self._text(data, "metric", where) or self._text(data, "rule", where),
            arguments=dict(arguments),
            thresholds={key: data[key] for key in THRESHOLDS if key in data},
            engine=self._text(data, "engine", where),
            implementation=data.get("implementation"),
            query=self._text(data, "query", where),
            raw=data,
        )

    def relationship(self, data: object, where: str, *, property_level: bool) -> Relationship:
        data = self._mapping(data, where)
        relationship_type = self._text(data, "type", where) or FOREIGN_KEY
        target = data.get("to")
        source = data.get("from")
        if property_level:
            if source is not None:
                raise self._error(where, "'from' is not allowed on a property: it is the property")
            if not isinstance(target, str) or not target:
                raise self._error(
                    where,
                    "'to' must reference a single property; declare composite keys on the "
                    "schema object with 'from' and 'to' lists",
                )
            return Relationship(type=relationship_type, target=(target,), raw=data)
        sources = self._references(source, f"{where}.from")
        targets = self._references(target, f"{where}.to")
        if len(sources) != len(targets):
            raise self._error(where, "'from' and 'to' must reference the same number of properties")
        return Relationship(type=relationship_type, target=targets, source=sources, raw=data)

    def _references(self, value: object, where: str) -> tuple[str, ...]:
        values = [value] if isinstance(value, str) else value
        if not isinstance(values, Sequence) or not values or not all(isinstance(item, str) and item for item in values):
            raise self._error(where, "must be a reference or a non-empty list of references")
        return tuple(values)

    def _quality_rules(self, data: Mapping[str, Any], where: str) -> tuple[QualityRule, ...]:
        return tuple(
            self.quality(item, f"{where}.quality[{index}]")
            for index, item in enumerate(self._list(data, "quality", where))
        )

    def _text(self, data: Mapping[str, Any], key: str, where: str) -> str | None:
        value = data.get(key)
        if value is None or value == "":
            return None
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise self._error(_join(where, key), f"must be a string, got {value!r}")
        return str(value)

    def _required_text(self, data: Mapping[str, Any], key: str, where: str) -> str:
        value = self._text(data, key, where)
        if value is None:
            raise self._error(_join(where, key), "is required")
        return value

    def _bool(self, data: Mapping[str, Any], key: str, where: str) -> bool:
        return self._optional_bool(data, key, where) or False

    def _optional_bool(self, data: Mapping[str, Any], key: str, where: str) -> bool | None:
        value = data.get(key)
        if value is None:
            return None
        if not isinstance(value, bool):
            raise self._error(_join(where, key), f"must be true or false, got {value!r}")
        return value

    def _position(self, data: Mapping[str, Any], key: str, where: str) -> int | None:
        value = data.get(key)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise self._error(_join(where, key), f"must be an integer, got {value!r}")
        # ODCS uses -1 for "no position".
        return value if value >= 0 else None

    def _list(self, data: Mapping[str, Any], key: str, where: str) -> list[Any]:
        value = data.get(key)
        if value is None:
            return []
        if not isinstance(value, list):
            raise self._error(_join(where, key), "must be a list")
        return value

    def _mapping(self, data: object, where: str) -> Mapping[str, Any]:
        if not isinstance(data, Mapping):
            raise self._error(where, "must be a mapping")
        return data

    def _tags(self, data: Mapping[str, Any], where: str) -> tuple[str, ...]:
        return tuple(str(tag) for tag in self._list(data, "tags", where))

    def _custom_properties(self, data: Mapping[str, Any], where: str) -> dict[str, Any]:
        properties: dict[str, Any] = {}
        for index, item in enumerate(self._list(data, "customProperties", where)):
            item = self._mapping(item, f"{_join(where, 'customProperties')}[{index}]")
            if "property" not in item:
                raise self._error(f"{_join(where, 'customProperties')}[{index}]", "requires 'property'")
            properties[str(item["property"])] = item.get("value")
        return properties

    def _check_unique(self, names: list[str], where: str, element: str) -> None:
        seen: set[str] = set()
        for name in names:
            if name.lower() in seen:
                raise self._error(where, f"the {element} {name!r} is declared more than once")
            seen.add(name.lower())

    def _error(self, where: str, message: str) -> ContractError:
        location = f"{self.source}: {where}" if where else self.source
        return ContractError(f"{location}: {message}")


def _join(where: str, key: str) -> str:
    return f"{where}.{key}" if where else key
