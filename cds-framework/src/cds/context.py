"""Bootstrap of the services."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from cds.config import NamingSettings, resolved
from cds.data_context import Contract, DataContext, DataProduct, Table
from cds.environment import Environment
from cds.errors import ConfigurationError
from cds.runtime import DBUtils, RuntimeConfiguration, environment_values, widget_values

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_PHYSICAL_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")


class ServiceContext:
    """Everything a service needs to run.

    It knows the environment, holds the Spark session, the data context (the known data
    products) and the runtime configuration, and resolves the physical names of the
    Unity Catalog objects.

    Args:
        spark: The Spark session. ``None`` is accepted for offline uses such as
            validating contracts; reading ``spark`` then raises an error.
        environment: The environment, or its id.
        data_context: The known data products.
        runtime_configuration: The runtime parameters.
        dbutils: The Databricks utilities of the notebook, if any.
        workspace_client: A Databricks ``WorkspaceClient``; created on first use if not given.
    """

    def __init__(
        self,
        *,
        spark: SparkSession | None,
        environment: Environment | str,
        data_context: DataContext,
        runtime_configuration: RuntimeConfiguration | None = None,
        dbutils: DBUtils | None = None,
        workspace_client: WorkspaceClient | None = None,
    ) -> None:
        self._spark = spark
        self.environment = environment if isinstance(environment, Environment) else Environment(environment)
        self.data_context = data_context
        self.runtime_configuration = runtime_configuration or RuntimeConfiguration({"environment": self.environment.id})
        self.dbutils = dbutils
        self._workspace_client = workspace_client

    @classmethod
    def create(
        cls,
        *,
        spark: SparkSession | None = None,
        dbutils: DBUtils | None = None,
        roots: str | Path | Iterable[str | Path] | None = None,
        parameters: Mapping[str, object] | None = None,
    ) -> ServiceContext:
        """Creates a context from the runtime parameters.

        Parameters are read, by order of precedence, from ``parameters``, the notebook
        widgets and job parameters (through ``dbutils``), and the ``CDS_*`` environment
        variables. ``environment`` is required; ``roots`` (comma-separated folders) is
        required unless given as an argument.

        Args:
            spark: The Spark session. If ``None``, the active session, or a Databricks
                Connect session when the package is installed.
            dbutils: The Databricks utilities of the notebook, if any.
            roots: The root folders of the data products.
            parameters: Explicit runtime parameters.
        """
        runtime = RuntimeConfiguration.from_sources(parameters, widget_values(dbutils), environment_values())
        if roots is None:
            roots = runtime.get_list("roots")
        elif isinstance(roots, (str, Path)):
            roots = [roots]
        roots = list(roots)
        if not roots:
            raise ConfigurationError("No data product root: pass roots=... or set the 'roots' runtime parameter.")
        environment = Environment(runtime.require("environment"))
        data_context = DataContext.discover(roots)
        return cls(
            spark=spark if spark is not None else default_spark_session(),
            environment=environment,
            data_context=data_context,
            runtime_configuration=runtime,
            dbutils=dbutils,
        )

    @property
    def spark(self) -> SparkSession:
        """The Spark session."""
        if self._spark is None:
            raise ConfigurationError("This service context has no Spark session.")
        return self._spark

    @property
    def has_spark(self) -> bool:
        return self._spark is not None

    @property
    def workspace_client(self) -> WorkspaceClient:
        """The Databricks ``WorkspaceClient``, created on first use."""
        if self._workspace_client is None:
            try:
                from databricks.sdk import WorkspaceClient
            except ImportError as exc:
                raise ConfigurationError("The Databricks SDK is required: install cds-framework[dqx].") from exc
            self._workspace_client = WorkspaceClient()
        return self._workspace_client

    def get_data_product(self, reference: str | None = None) -> DataProduct:
        """Returns a data product, by default the one set in the runtime parameters.

        The default comes from the ``data_product`` parameter (``<group>.<data_product>``),
        optionally combined with a ``group`` parameter.

        Raises:
            ConfigurationError: If no data product is set, or if it is unknown.
        """
        if reference is None:
            reference = self.runtime_configuration.get("data_product")
            group = self.runtime_configuration.get("group")
            if reference and group and "." not in reference:
                reference = f"{group}.{reference}"
        if not reference:
            raise ConfigurationError(
                "No data product to process: set the 'data_product' runtime parameter to <group>.<data_product>."
            )
        return self.data_context.get_data_product(reference)

    def catalog_name(self, element: DataProduct | Contract | Table) -> str:
        """Returns the catalog of a data product, contract or table."""
        return self._resolve(element, "catalog")

    def schema_name(self, element: DataProduct | Contract | Table) -> str:
        """Returns the schema (database) of a data product, contract or table."""
        return self._resolve(element, "schema")

    def full_schema_name(self, element: DataProduct | Contract | Table) -> str:
        """Returns the unquoted ``catalog.schema`` name of a data product, contract or table."""
        return f"{self.catalog_name(element)}.{self.schema_name(element)}"

    def full_table_name(self, table: Table, name: str | None = None) -> str:
        """Returns the unquoted ``catalog.schema.table`` name of a table.

        Args:
            table: The table.
            name: Name of another table of the same schema, e.g. its history table.
        """
        table_name = name or table.name
        _check_physical_name(table_name, "table", table.qualified_name)
        return f"{self.full_schema_name(table)}.{table_name}"

    def _resolve(self, element: DataProduct | Contract | Table, part: Literal["catalog", "schema"]) -> str:
        if isinstance(element, DataProduct):
            contract: Contract | None = None
            data_product = element
        else:
            contract = element.contract if isinstance(element, Table) else element
            data_product = contract.data_product
        settings = (contract or data_product).settings(NamingSettings)
        values = {
            "environment": self.environment.id,
            "group": data_product.group.name,
            "data_product": data_product.name,
        }
        if contract is not None:
            values["contract"] = contract.name
        pattern = resolved(settings.catalog if part == "catalog" else settings.schema)
        try:
            name = pattern.format_map(values)
        except (KeyError, IndexError, ValueError) as exc:
            raise ConfigurationError(
                f"Invalid {part} naming pattern {pattern!r} for {data_product.qualified_name}: "
                f"{exc}. Available placeholders: {', '.join(values)}."
            ) from None
        _check_physical_name(name, part, data_product.qualified_name)
        return name


def _check_physical_name(name: str, part: str, owner: str) -> None:
    if not _PHYSICAL_NAME.match(name):
        raise ConfigurationError(f"Invalid {part} name {name!r} for {owner}.")


def default_spark_session() -> SparkSession:
    """Returns the active Spark session, or a Databricks Connect session."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ConfigurationError("PySpark is required to run services.") from exc
    session = SparkSession.getActiveSession()
    if session is not None:
        return session
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        return SparkSession.builder.getOrCreate()
    logger.info("Creating a Databricks Connect session")
    return DatabricksSession.builder.getOrCreate()
