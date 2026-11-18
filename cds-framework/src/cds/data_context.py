"""Discovery of the data products of a project, and lookup of their tables.

A project is made of one or more root folders following this layout::

    <root>/
        <group>/
            <group>_config.py                   (optional)
            <group>.odcs.yaml                   (optional partial ODCS document)
            <data_product>/
                <data_product>_config.py        (optional)
                <data_product>.odcs.yaml        (optional partial ODCS document)
                <contract>/
                    <contract>.yaml             (ODCS data contract)
                    <contract>_config.py        (optional)

Other files and folders are ignored, so configuration files can share helper modules.

The contract defaults of a group or data product, merged under each of its contracts,
are its partial ODCS document (``cds.odcs.read_partial``), or else the
``contract_defaults`` of its configuration.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any, TypeVar

from cds.config import ContractConfig, DataProductConfig, GroupConfig, SettingsT, resolve_settings
from cds.errors import ConfigurationError, ContractError
from cds.loader import CONFIG_SUFFIX, ModuleLoader, load_config
from cds.odcs import DataContract, SchemaObject, SchemaProperty, merge_documents, partial_document, read_partial

logger = logging.getLogger(__name__)

NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
CONTRACT_SUFFIXES = (".yaml", ".yml")
# Suffixes of the partial ODCS document of a group or data product: its contract defaults.
DEFAULTS_SUFFIXES = (".odcs.yaml", ".odcs.yml")

# Fully qualified ODCS reference to a property of the same contract, by ids.
_ID_REFERENCE = re.compile(
    r"^(?:(?P<file>[^#]+)#)?/?schema/(?P<schema>[A-Za-z0-9_-]+)/properties/(?P<property>[A-Za-z0-9_-]+)$"
)

T = TypeVar("T")


class Group:
    """A group of data products, the first level of the project layout.

    Attributes:
        contract_defaults: Partial ODCS document merged under every contract of the group,
            loaded with the group.
    """

    def __init__(self, name: str, path: Path, config_path: Path | None, defaults_path: Path | None) -> None:
        self.name = name
        self.path = path
        self.config_path = config_path
        self.defaults_path = defaults_path
        self.config: object | None = None
        self.contract_defaults: dict[str, Any] = {}
        self.data_products: list[DataProduct] = []

    def get_data_product(self, name: str) -> DataProduct:
        """Returns a data product of the group.

        Raises:
            ConfigurationError: If the group has no such data product.
        """
        for data_product in self.data_products:
            if data_product.name == name:
                return data_product
        raise ConfigurationError(
            f"The group {self.name!r} has no data product {name!r}. Data products: "
            f"{', '.join(dp.name for dp in self.data_products) or 'none'}."
        )

    def _load(self, loader: ModuleLoader) -> None:
        if self.config_path:
            self.config = load_config(loader, self.config_path, self.name, GroupConfig)
        self.contract_defaults = _contract_defaults(self.config, self.config_path, self.defaults_path)
        for data_product in self.data_products:
            data_product._load(loader)

    def __repr__(self) -> str:
        return f"Group({self.name!r})"


class DataProduct:
    """A data product: a set of contracts, published in the same schema by default.

    Attributes:
        contract_defaults: Partial ODCS document merged under every contract of the data
            product: the defaults of its group, then its own. Loaded with the data product.
    """

    def __init__(
        self, name: str, path: Path, group: Group, config_path: Path | None, defaults_path: Path | None
    ) -> None:
        self.name = name
        self.path = path
        self.group = group
        self.config_path = config_path
        self.defaults_path = defaults_path
        self.config: object | None = None
        self.contract_defaults: dict[str, Any] = {}
        self.contracts: list[Contract] = []
        self._settings: dict[type, object] = {}

    @property
    def qualified_name(self) -> str:
        """Name of the data product prefixed by its group: ``<group>.<data_product>``."""
        return f"{self.group.name}.{self.name}"

    @property
    def tables(self) -> list[Table]:
        """The tables of all the contracts of the data product."""
        return [table for contract in self.contracts for table in contract.tables]

    @property
    def description(self) -> str | None:
        return getattr(self.config, "description", None)

    def get_table(self, name: str) -> Table:
        """Returns a table of the data product.

        Raises:
            ConfigurationError: If the data product has no such table.
        """
        lowered = name.lower()
        for table in self.tables:
            if table.name.lower() == lowered:
                return table
        raise ConfigurationError(
            f"The data product {self.qualified_name!r} has no table {name!r}. Tables: "
            f"{', '.join(table.name for table in self.tables) or 'none'}."
        )

    def settings(self, kind: type[SettingsT]) -> SettingsT:
        """Resolves the settings of the data product, inherited from its group."""
        cached = self._settings.get(kind)
        if isinstance(cached, kind):
            return cached
        settings = resolve_settings(kind, self.config, self.group.config)
        self._settings[kind] = settings
        return settings

    def _load(self, loader: ModuleLoader) -> None:
        if self.config_path:
            self.config = load_config(loader, self.config_path, self.name, DataProductConfig)
        self.contract_defaults = merge_documents(
            self.group.contract_defaults, _contract_defaults(self.config, self.config_path, self.defaults_path)
        )
        for contract in self.contracts:
            contract._load(loader, self.contract_defaults)
        seen: dict[str, Contract] = {}
        for table in self.tables:
            other = seen.setdefault(table.name.lower(), table.contract)
            if other is not table.contract:
                raise ConfigurationError(
                    f"The table {table.name!r} is declared by the contracts {other.name!r} and "
                    f"{table.contract.name!r} of the data product {self.qualified_name!r}."
                )

    def __repr__(self) -> str:
        return f"DataProduct({self.qualified_name!r})"


class Contract:
    """A data contract of a data product: its ODCS document and its configuration."""

    def __init__(
        self, name: str, path: Path, data_product: DataProduct, document_path: Path, config_path: Path | None
    ) -> None:
        self.name = name
        self.path = path
        self.data_product = data_product
        self.document_path = document_path
        self.config_path = config_path
        self.config: object | None = None
        self.document: DataContract | None = None
        self.tables: list[Table] = []
        self._settings: dict[type, object] = {}

    @property
    def qualified_name(self) -> str:
        return f"{self.data_product.qualified_name}.{self.name}"

    def settings(self, kind: type[SettingsT]) -> SettingsT:
        """Resolves the settings of the contract, inherited from its data product and group."""
        cached = self._settings.get(kind)
        if isinstance(cached, kind):
            return cached
        settings = resolve_settings(kind, self.config, self.data_product.config, self.data_product.group.config)
        self._settings[kind] = settings
        return settings

    def _load(self, loader: ModuleLoader, defaults: dict[str, Any]) -> None:
        if self.config_path:
            self.config = load_config(loader, self.config_path, self.name, ContractConfig)
        self.document = DataContract.from_file(self.document_path, defaults=defaults)
        self.tables = [Table(self, schema_object) for schema_object in self.document.schema]

    def __repr__(self) -> str:
        return f"Contract({self.qualified_name!r})"


class Table:
    """A table: a schema object of a contract."""

    def __init__(self, contract: Contract, schema_object: SchemaObject) -> None:
        self.contract = contract
        self.schema_object = schema_object

    @property
    def name(self) -> str:
        return self.schema_object.table_name

    @property
    def data_product(self) -> DataProduct:
        return self.contract.data_product

    @property
    def group(self) -> Group:
        return self.contract.data_product.group

    @property
    def qualified_name(self) -> str:
        """Name of the table prefixed by its group and data product."""
        return f"{self.data_product.qualified_name}.{self.name}"

    @property
    def description(self) -> str | None:
        return self.schema_object.description

    @property
    def columns(self) -> tuple[SchemaProperty, ...]:
        return self.schema_object.properties

    @property
    def column_names(self) -> list[str]:
        return [prop.column for prop in self.schema_object.properties]

    @property
    def primary_key(self) -> tuple[str, ...]:
        return self.schema_object.primary_key

    def get_column(self, name: str) -> SchemaProperty | None:
        return self.schema_object.get_property(name)

    def settings(self, kind: type[SettingsT]) -> SettingsT:
        """Resolves the settings of the table, those of its contract."""
        return self.contract.settings(kind)

    def __repr__(self) -> str:
        return f"Table({self.qualified_name!r})"


class DataContext:
    """The known data products, and the tables of their contracts."""

    def __init__(self, groups: Sequence[Group]) -> None:
        self.groups = list(groups)

    @classmethod
    def discover(cls, roots: str | Path | Iterable[str | Path]) -> DataContext:
        """Discovers and loads the data products of one or more root folders.

        Configuration files and contracts are loaded eagerly, so that any error is
        reported before a service starts.

        Raises:
            ConfigurationError: If the layout or a configuration file is invalid.
            ContractError: If a contract is invalid.
        """
        if isinstance(roots, (str, Path)):
            roots = [roots]
        groups: dict[str, Group] = {}
        for root in roots:
            root_path = Path(root)
            if not root_path.is_dir():
                raise ConfigurationError(f"The data product root {root} is not a folder.")
            loader = ModuleLoader(root_path)
            for directory in _subdirectories(root_path):
                group = _scan_group(directory)
                if group is None:
                    continue
                if group.name in groups:
                    raise ConfigurationError(
                        f"The group {group.name!r} is defined in {groups[group.name].path} and {group.path}."
                    )
                group._load(loader)
                groups[group.name] = group
        context = cls(list(groups.values()))
        logger.info(
            "Data context: %s group(s), %s data product(s), %s table(s)",
            len(context.groups),
            len(context.data_products),
            len(context.tables),
        )
        return context

    @property
    def data_products(self) -> list[DataProduct]:
        return [data_product for group in self.groups for data_product in group.data_products]

    @property
    def contracts(self) -> list[Contract]:
        return [contract for data_product in self.data_products for contract in data_product.contracts]

    @property
    def tables(self) -> list[Table]:
        return [table for data_product in self.data_products for table in data_product.tables]

    def get_group(self, name: str) -> Group:
        for group in self.groups:
            if group.name == name:
                return group
        raise ConfigurationError(f"Unknown group {name!r}. Groups: {', '.join(g.name for g in self.groups) or 'none'}.")

    def get_data_product(self, reference: str) -> DataProduct:
        """Returns a data product from ``<group>.<data_product>``, or its unique name.

        Raises:
            ConfigurationError: If the data product is unknown or its name is ambiguous.
        """
        if "." in reference:
            group_name, _, name = reference.partition(".")
            return self.get_group(group_name).get_data_product(name)
        matches = [dp for dp in self.data_products if dp.name == reference]
        return _single(matches, f"data product {reference!r}", lambda dp: dp.qualified_name)

    def find_tables(self, name: str) -> list[Table]:
        """Returns all the tables with this name, in any data product."""
        lowered = name.lower()
        return [table for table in self.tables if table.name.lower() == lowered]

    def get_table(self, reference: str, origin: Table | None = None) -> Table:
        """Returns the table designated by a reference.

        The reference is ``<table>``, ``<data_product>.<table>`` or
        ``<group>.<data_product>.<table>``. A table name alone is first looked up in the
        contract of ``origin``, then in its data product, then in every data product.

        Raises:
            ConfigurationError: If no table, or more than one table, matches.
        """
        parts = reference.split(".")
        if len(parts) > 3 or not all(parts):
            raise ConfigurationError(f"Invalid table reference {reference!r}.")
        candidates = self.find_tables(parts[-1])
        if len(parts) >= 2:
            candidates = [t for t in candidates if t.data_product.name == parts[-2]]
        if len(parts) == 3:
            candidates = [t for t in candidates if t.group.name == parts[0]]
        if len(parts) == 1 and origin is not None and len(candidates) > 1:
            scopes: tuple[Callable[[Table], bool], ...] = (
                lambda t: t.contract is origin.contract,
                lambda t: t.data_product is origin.data_product,
            )
            for scope in scopes:
                scoped = [table for table in candidates if scope(table)]
                if scoped:
                    candidates = scoped
                    break
        return _single(candidates, f"table {reference!r}", lambda table: table.qualified_name)

    def resolve_reference(self, reference: str, origin: Table) -> tuple[Table, str]:
        """Resolves an ODCS reference to a property, from the table that declares it.

        Two notations are supported: the shorthand ``[[<group>.]<data_product>.]<table>.<property>``,
        and the fully qualified ``schema/<id>/properties/<id>`` within the contract of
        ``origin``.

        Returns:
            The referenced table and the name of the referenced column.

        Raises:
            ContractError: If the reference is invalid or designates an unknown property.
        """
        qualified = _ID_REFERENCE.match(reference)
        if qualified:
            if qualified.group("file"):
                raise ContractError(
                    f"{origin.qualified_name}: references to other files ({reference!r}) are not "
                    "supported; use the shorthand notation <table>.<property>."
                )
            return self._resolve_id_reference(qualified, reference, origin)
        table_reference, _, column = reference.rpartition(".")
        if not table_reference or not column:
            raise ContractError(
                f"{origin.qualified_name}: invalid reference {reference!r}, expected <table>.<property>."
            )
        try:
            table = self.get_table(table_reference, origin=origin)
        except ConfigurationError as exc:
            raise ContractError(f"{origin.qualified_name}: reference {reference!r}: {exc}") from None
        prop = table.get_column(column)
        if prop is None:
            raise ContractError(
                f"{origin.qualified_name}: reference {reference!r}: the table {table.qualified_name} "
                f"has no property {column!r}."
            )
        return table, prop.column

    def _resolve_id_reference(self, match: re.Match[str], reference: str, origin: Table) -> tuple[Table, str]:
        for table in origin.contract.tables:
            if table.schema_object.id == match.group("schema"):
                for prop in table.columns:
                    if prop.id == match.group("property"):
                        return table, prop.column
        raise ContractError(
            f"{origin.qualified_name}: reference {reference!r} designates no property of the "
            f"contract {origin.contract.name!r}."
        )


def _single(items: list[T], description: str, describe: Callable[[T], str]) -> T:
    if not items:
        raise ConfigurationError(f"Unknown {description}.")
    if len(items) > 1:
        raise ConfigurationError(
            f"Ambiguous {description}: it matches {', '.join(describe(item) for item in items)}. "
            "Qualify it with its data product."
        )
    return items[0]


def _subdirectories(path: Path) -> list[Path]:
    """Returns the folders that may be elements of the layout, by name."""
    folders = []
    for child in sorted(path.iterdir()):
        if not child.is_dir() or child.name.startswith((".", "_")):
            continue
        if not NAME_PATTERN.match(child.name):
            if any(
                (child / f"{child.name}{suffix}").exists()
                for suffix in (CONFIG_SUFFIX, *DEFAULTS_SUFFIXES, *CONTRACT_SUFFIXES)
            ):
                raise ConfigurationError(
                    f"Invalid name {child.name!r} ({child}): names must start with a letter and "
                    "contain only letters, digits and underscores."
                )
            continue
        folders.append(child)
    return folders


def _own_file(directory: Path, *suffixes: str) -> Path | None:
    """Returns the first existing file named after a folder with one of the suffixes, such as its configuration file."""
    paths = (directory / f"{directory.name}{suffix}" for suffix in suffixes)
    return next((path for path in paths if path.is_file()), None)


def _contract_defaults(config: object | None, config_path: Path | None, defaults_path: Path | None) -> dict[str, Any]:
    """The contract defaults of a group or data product: its partial ODCS document, or those of its configuration.

    Raises:
        ConfigurationError: If both define contract defaults.
        ContractError: If the contract defaults are not a valid partial ODCS document.
    """
    configured = getattr(config, "contract_defaults", None)
    if defaults_path is None:
        return partial_document(configured, f"{config_path}: contract_defaults")
    if configured:
        raise ConfigurationError(
            f"{defaults_path} and the contract_defaults of the configuration both define the contract defaults: "
            "keep one of them."
        )
    return read_partial(defaults_path)


def _scan_group(directory: Path) -> Group | None:
    group = Group(
        directory.name, directory, _own_file(directory, CONFIG_SUFFIX), _own_file(directory, *DEFAULTS_SUFFIXES)
    )
    for child in _subdirectories(directory):
        data_product = _scan_data_product(child, group)
        if data_product is not None:
            group.data_products.append(data_product)
    if not group.data_products and group.config_path is None and group.defaults_path is None:
        return None
    return group


def _scan_data_product(directory: Path, group: Group) -> DataProduct | None:
    data_product = DataProduct(
        directory.name, directory, group, _own_file(directory, CONFIG_SUFFIX), _own_file(directory, *DEFAULTS_SUFFIXES)
    )
    for child in _subdirectories(directory):
        contract = _scan_contract(child, data_product)
        if contract is not None:
            data_product.contracts.append(contract)
    if not data_product.contracts and data_product.config_path is None and data_product.defaults_path is None:
        return None
    return data_product


def _scan_contract(directory: Path, data_product: DataProduct) -> Contract | None:
    config_path = _own_file(directory, CONFIG_SUFFIX)
    document_path = _own_file(directory, *CONTRACT_SUFFIXES)
    if document_path is None:
        if config_path is not None:
            raise ConfigurationError(
                f"{config_path} configures a contract, but {directory / (directory.name + '.yaml')} does not exist."
            )
        return None
    return Contract(directory.name, directory, data_product, document_path, config_path)
