"""CDS (Contract Driven Services): build data products from ODCS data contracts.

A project describes its data products with ODCS data contracts and Python
configuration files, in a ``<group>/<data_product>/<contract>`` layout. The services
implement the data products from the contracts: schema, metadata, transformation,
quality (DQX), SCD and snapshot, and the Genie spaces of the data products (genie).
``run_service`` runs a service from a Databricks notebook or job.
"""

from importlib.metadata import PackageNotFoundError, version

from cds.config import (
    ContractConfig,
    DataProductConfig,
    GenieExample,
    GenieSettings,
    GroupConfig,
    MetadataSettings,
    NamingSettings,
    QualitySettings,
    ScdSettings,
    SchemaSettings,
    SnapshotSettings,
)
from cds.context import ServiceContext
from cds.data_context import Contract, DataContext, DataProduct, Group, Table
from cds.environment import Environment
from cds.errors import CdsError, ConfigurationError, ContractError, InjectionError, ServiceError
from cds.injection import ContractInjection
from cds.runner import run_service
from cds.runtime import RuntimeConfiguration
from cds.services import (
    GenieService,
    MetadataService,
    QualityService,
    ScdService,
    SchemaService,
    Service,
    ServiceReport,
    SnapshotService,
    Status,
    TableResult,
    TransformationService,
)
from cds.services.checks import literal_values

try:
    __version__ = version("cds-framework")
except PackageNotFoundError:  # pragma: no cover - running from the sources
    __version__ = "0.0.0"

__all__ = [
    "CdsError",
    "ConfigurationError",
    "Contract",
    "ContractConfig",
    "ContractError",
    "ContractInjection",
    "DataContext",
    "DataProduct",
    "DataProductConfig",
    "Environment",
    "GenieExample",
    "GenieService",
    "GenieSettings",
    "Group",
    "GroupConfig",
    "InjectionError",
    "MetadataService",
    "MetadataSettings",
    "NamingSettings",
    "QualityService",
    "QualitySettings",
    "RuntimeConfiguration",
    "ScdService",
    "ScdSettings",
    "SchemaService",
    "SchemaSettings",
    "Service",
    "ServiceContext",
    "ServiceError",
    "ServiceReport",
    "SnapshotService",
    "SnapshotSettings",
    "Status",
    "Table",
    "TableResult",
    "TransformationService",
    "literal_values",
    "run_service",
]
