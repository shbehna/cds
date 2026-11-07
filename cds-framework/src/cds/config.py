"""Configuration of groups, data products and contracts.

Each level of the project layout (group, data product, contract) may have a
configuration file defining a ``config`` object. Settings follow "convention over
configuration": a setting left unset on a contract is inherited from its data product,
then from its group, and finally from the framework defaults (``DEFAULTS``). A setting
left to ``None`` is unset.

Consumers may use the configuration classes as they are, extend them with their own
attributes and methods, or provide any object exposing the same attributes.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import Field, dataclass, fields
from typing import Any, ClassVar, Protocol, TypeVar

from cds.errors import ConfigurationError

T = TypeVar("T")


class Settings(Protocol):
    """The settings classes: frozen dataclasses naming the configuration attribute holding them."""

    attribute: ClassVar[str]
    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


SettingsT = TypeVar("SettingsT", bound=Settings)

CRITICALITIES = ("error", "warn")

# Kinds of quality checks inferred from the contracts, see QualitySettings.criticality.
CHECK_KINDS = ("required", "primary_key", "unique", "foreign_key", "enum", "pattern", "range")


def _check_choice(owner: object, name: str, choices: Sequence[str]) -> None:
    value = getattr(owner, name)
    if value is not None and value not in choices:
        raise ConfigurationError(f"{type(owner).__name__}.{name} must be one of {', '.join(choices)}, got {value!r}.")


@dataclass(frozen=True)
class NamingSettings:
    """Physical names of the Unity Catalog objects of a data product.

    The patterns are Python format strings using the placeholders ``{environment}``,
    ``{group}``, ``{data_product}`` and ``{contract}``.

    Attributes:
        catalog: Pattern of the catalog name. Default: ``{environment}``.
        schema: Pattern of the schema name. Default: ``{group}_{data_product}``.
    """

    attribute: ClassVar[str] = "naming"

    catalog: str | None = None
    schema: str | None = None


@dataclass(frozen=True)
class SchemaSettings:
    """Settings of the schema service.

    Attributes:
        on_breaking_change: What to do with an existing table whose columns were removed
            or retyped in its contract: ``replace`` recreates the table, losing its data
            (it remains reachable with Delta time travel); ``error`` leaves the table
            untouched and reports an error. Added columns never need either. Default:
            ``replace``.
    """

    attribute: ClassVar[str] = "schema"

    on_breaking_change: str | None = None

    def __post_init__(self) -> None:
        _check_choice(self, "on_breaking_change", ("replace", "error"))


@dataclass(frozen=True)
class QualitySettings:
    """Settings of the quality (DQX) service.

    Attributes:
        enabled: Whether the tables are checked. Default: ``True``.
        criticality: DQX criticality (``error`` or ``warn``) of the checks inferred from
            the contracts, by kind: ``required``, ``primary_key``, ``unique``,
            ``foreign_key``, ``enum``, ``pattern`` and ``range``. Levels are merged kind
            by kind. Default: ``error`` for every kind.
        errors_column: Column holding the errors of each row. Default: ``_errors``.
        warnings_column: Column holding the warnings of each row. Default: ``_warnings``.
        info_column: Column holding the DQX information of each row. Default: ``_dq_info``.
    """

    attribute: ClassVar[str] = "quality"

    enabled: bool | None = None
    criticality: Mapping[str, str] | None = None
    errors_column: str | None = None
    warnings_column: str | None = None
    info_column: str | None = None

    def __post_init__(self) -> None:
        for kind, criticality in (self.criticality or {}).items():
            if kind not in CHECK_KINDS:
                raise ConfigurationError(
                    f"QualitySettings.criticality: unknown check kind {kind!r}, expected one of "
                    f"{', '.join(CHECK_KINDS)}."
                )
            if criticality not in CRITICALITIES:
                raise ConfigurationError(
                    f"QualitySettings.criticality[{kind!r}] must be 'error' or 'warn', got {criticality!r}."
                )

    @property
    def result_columns(self) -> tuple[str, ...]:
        """The columns added to a table by the quality service."""
        return tuple(column for column in (self.errors_column, self.warnings_column, self.info_column) if column)


@dataclass(frozen=True)
class ScdSettings:
    """Settings of the SCD (slowly changing dimension, type 2) service.

    Attributes:
        enabled: Whether the tables with a primary key are historized. Default: ``True``.
        table: Pattern of the name of the history table, in the schema of the source
            table, with the placeholder ``{table}``. Default: ``{table}_history``.
        on_duplicate_keys: What to do when the source has several rows for a key:
            ``fail`` reports an error; ``deduplicate`` keeps one row per key, chosen
            deterministically, and reports a warning. Default: ``fail``.
        valid_from_column: Start of validity of a version. Default: ``_valid_from``.
        valid_to_column: End of validity of a version. Default: ``_valid_to``.
        current_column: Whether a version is the current one. Default: ``_is_current``.
        hash_column: Hash of the tracked columns of a version. Default: ``_row_hash``.
    """

    attribute: ClassVar[str] = "scd"

    enabled: bool | None = None
    table: str | None = None
    on_duplicate_keys: str | None = None
    valid_from_column: str | None = None
    valid_to_column: str | None = None
    current_column: str | None = None
    hash_column: str | None = None

    def __post_init__(self) -> None:
        _check_choice(self, "on_duplicate_keys", ("fail", "deduplicate"))


@dataclass(frozen=True)
class SnapshotSettings:
    """Settings of the snapshot service.

    Attributes:
        enabled: Whether the table is loaded with the latest record of each key of a
            historical table. Default: ``False``.
        source: Name of the historical table: a pattern with the placeholder ``{table}``
            for a table of the same schema, or a fully qualified ``catalog.schema.table``
            name. Default: the history table of the SCD service.
        order_by: SQL ordering expressions, the first row of each key being the latest.
            Default: the start of validity of the SCD service, descending.
        condition: Optional SQL condition filtering the historical rows.
    """

    attribute: ClassVar[str] = "snapshot"

    enabled: bool | None = None
    source: str | None = None
    order_by: Sequence[str] | None = None
    condition: str | None = None


DEFAULTS: Mapping[type[Settings], Settings] = {
    NamingSettings: NamingSettings(catalog="{environment}", schema="{group}_{data_product}"),
    SchemaSettings: SchemaSettings(on_breaking_change="replace"),
    QualitySettings: QualitySettings(
        enabled=True,
        criticality=dict.fromkeys(CHECK_KINDS, "error"),
        errors_column="_errors",
        warnings_column="_warnings",
        info_column="_dq_info",
    ),
    ScdSettings: ScdSettings(
        enabled=True,
        table="{table}_history",
        on_duplicate_keys="fail",
        valid_from_column="_valid_from",
        valid_to_column="_valid_to",
        current_column="_is_current",
        hash_column="_row_hash",
    ),
    SnapshotSettings: SnapshotSettings(enabled=False),
}


def resolve_settings(kind: type[SettingsT], *configs: object) -> SettingsT:
    """Resolves the effective settings of a kind.

    Args:
        kind: The settings class, e.g. ``QualitySettings``.
        *configs: Configuration objects, from the most specific to the most generic;
            ``None`` entries are ignored.

    Returns:
        The settings with every attribute set: for each attribute, the most specific
        configuration that sets it wins, falling back to ``DEFAULTS``. Mapping attributes
        are merged key by key.
    """
    layers: list[object] = [_settings_of(config, kind) for config in configs if config is not None]
    layers.append(DEFAULTS[kind])
    values: dict[str, Any] = {}
    for settings_field in fields(kind):
        result: Any = None
        for layer in reversed(layers):
            value = getattr(layer, settings_field.name) if layer is not None else None
            if value is None:
                continue
            merge = isinstance(value, Mapping) and isinstance(result, Mapping)
            result = {**result, **value} if merge else value
        values[settings_field.name] = result
    return kind(**values)


def resolved(value: T | None) -> T:
    """Returns an attribute of resolved settings, typed as set.

    Resolved settings (``resolve_settings``, ``Table.settings``) set every attribute that
    has a default: the attributes are optional only so that configurations can leave
    them unset.

    Raises:
        ConfigurationError: If the attribute is not set, i.e. the settings were not resolved.
    """
    if value is None:
        raise ConfigurationError("A setting is not set: settings must be resolved with resolve_settings.")
    return value


def _settings_of(config: object, kind: type[SettingsT]) -> SettingsT | None:
    attribute = kind.attribute
    value = getattr(config, attribute, None)
    if value is None or isinstance(value, kind):
        return value
    if isinstance(value, Mapping):
        try:
            return kind(**value)
        except TypeError as exc:
            raise ConfigurationError(f"Invalid {attribute} settings {dict(value)!r}: {exc}") from exc
    raise ConfigurationError(
        f"The {attribute!r} attribute of {config!r} must be a {kind.__name__} or a mapping, got {type(value).__name__}."
    )


class _LevelConfig:
    """Attributes shared by the configurations of the three levels.

    Attributes:
        name: Name of the element. It must match the name of its folder.
        description: Description of the element.
        naming: Naming settings.
        schema: Settings of the schema service.
        quality: Settings of the quality service.
        scd: Settings of the SCD service.
        snapshot: Settings of the snapshot service.
    """

    name: str | None = None
    description: str | None = None
    naming: NamingSettings | None = None
    schema: SchemaSettings | None = None
    quality: QualitySettings | None = None
    scd: ScdSettings | None = None
    snapshot: SnapshotSettings | None = None

    def _assign(self, values: Mapping[str, Any]) -> None:
        # Only set the given values: the class attributes of a subclass remain the
        # defaults of its instances.
        for key, value in values.items():
            if key not in ("self", "__class__") and value is not None:
                setattr(self, key, value)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"


class GroupConfig(_LevelConfig):
    """Configuration of a group, in ``<group>/<group>_config.py``.

    Attributes:
        contract_defaults: Partial ODCS document merged under every contract of the
            group (see ``cds.odcs.merge_documents``). The data product defaults and the
            contracts themselves take precedence.
    """

    contract_defaults: Mapping[str, Any] | None = None

    def __init__(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        naming: NamingSettings | None = None,
        schema: SchemaSettings | None = None,
        quality: QualitySettings | None = None,
        scd: ScdSettings | None = None,
        snapshot: SnapshotSettings | None = None,
        contract_defaults: Mapping[str, Any] | None = None,
    ) -> None:
        self._assign(locals())


class DataProductConfig(_LevelConfig):
    """Configuration of a data product, in ``<group>/<data_product>/<data_product>_config.py``.

    Attributes:
        contract_defaults: Partial ODCS document merged under every contract of the data
            product, over the defaults of the group.
    """

    contract_defaults: Mapping[str, Any] | None = None

    def __init__(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        naming: NamingSettings | None = None,
        schema: SchemaSettings | None = None,
        quality: QualitySettings | None = None,
        scd: ScdSettings | None = None,
        snapshot: SnapshotSettings | None = None,
        contract_defaults: Mapping[str, Any] | None = None,
    ) -> None:
        self._assign(locals())


class ContractConfig(_LevelConfig):
    """Configuration of a contract, in ``.../<contract>/<contract>_config.py``.

    Attributes:
        transform: Transformation producing the content of the table, called with
            injected parameters (see ``cds.injection.ContractInjection``) and returning a
            DataFrame. For a contract describing several tables, a mapping of table name
            to transformation. A subclass may define it as a method.
        quality_checks: Additional DQX checks, as metadata dictionaries: a list, a
            mapping of table name to list, or a function returning either, called with
            injected parameters.
    """

    transform: Callable[..., Any] | Mapping[str, Callable[..., Any]] | None = None
    quality_checks: (
        Sequence[Mapping[str, Any]] | Mapping[str, Sequence[Mapping[str, Any]]] | Callable[..., Any] | None
    ) = None

    def __init__(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        naming: NamingSettings | None = None,
        schema: SchemaSettings | None = None,
        quality: QualitySettings | None = None,
        scd: ScdSettings | None = None,
        snapshot: SnapshotSettings | None = None,
        transform: Callable[..., Any] | Mapping[str, Callable[..., Any]] | None = None,
        quality_checks: (
            Sequence[Mapping[str, Any]] | Mapping[str, Sequence[Mapping[str, Any]]] | Callable[..., Any] | None
        ) = None,
    ) -> None:
        self._assign(locals())
