"""Base class of the services creating a workspace object of a data product, such as its Genie space."""

from __future__ import annotations

import logging
from abc import abstractmethod
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import quote

from cds.errors import ConfigurationError
from cds.services.base import Service, ServiceReport, TableResult
from cds.sql import quote_name, string_literal

if TYPE_CHECKING:
    from cds.data_context import DataProduct, Table

logger = logging.getLogger(__name__)

SCHEMAS_PATH = "/api/2.1/unity-catalog/schemas"
WORKSPACE_PREFIX = "/Workspace"


class DataProductService(Service):
    """A service processing a data product as a whole, not its tables.

    It reports a single result, named after the data product. The workspace object it
    creates is recorded in a property of the schema of the data product, which the next
    runs read to update it.
    """

    # What the service creates, for its messages, such as "Genie space".
    object_name: ClassVar[str]

    def run(
        self,
        data_product: DataProduct,
        tables: Iterable[str] | None = None,
        max_workers: int | None = None,
    ) -> ServiceReport:
        """Processes a data product; ``tables`` and ``max_workers`` are ignored."""
        if tables:
            logger.info(
                "%s: the %s has all the tables of %s, not only %s",
                self.name,
                self.object_name,
                data_product.qualified_name,
                tables,
            )
        try:
            result = self.synchronize(data_product)
        except Exception as exc:
            logger.exception("%s: the %s of %s failed", self.name, self.object_name, data_product.qualified_name)
            result = TableResult.failure(data_product.qualified_name, f"{type(exc).__name__}: {exc}", exc)
        return ServiceReport(self.name, data_product.qualified_name, [result])

    def process(self, table: Table) -> TableResult:
        raise ConfigurationError(f"The {self.name} service processes data products, not tables: call run.")

    @abstractmethod
    def synchronize(self, data_product: DataProduct) -> TableResult:
        """Creates or updates the workspace object of a data product, and returns its result."""

    @property
    def api(self) -> Any:
        """The REST client of the workspace: it does not depend on the version of the SDK."""
        return self.context.workspace_client.api_client

    def recorded_id(self, schema: str, key: str) -> str | None:
        """The id recorded in a property of a schema, if any."""
        found = self.api.do("GET", f"{SCHEMAS_PATH}/{quote(schema, safe='')}")
        properties = (found.get("properties") if isinstance(found, Mapping) else None) or {}
        value = properties.get(key)
        return str(value) if value else None

    def record_id(self, schema: str, key: str, value: str, delete_path: str) -> None:
        """Records the id of a new object in a property of a schema.

        If it cannot be recorded, the object is deleted with ``DELETE <delete_path>``, and
        the error raised: the next run would not find the object, and would create another.
        """
        try:
            self.spark.sql(
                f"ALTER SCHEMA {quote_name(schema)} SET DBPROPERTIES ({string_literal(key)} = {string_literal(value)})"
            )
        except Exception:
            logger.error("%s: cannot record the %s %s in %s: deleting it", self.name, self.object_name, value, schema)
            try:
                self.api.do("DELETE", delete_path)
            except Exception:
                logger.exception("%s: cannot delete the %s %s", self.name, self.object_name, value)
            raise

    def get_or_none(self, path: str, query: Mapping[str, str] | None = None) -> Mapping[str, Any] | None:
        """The object at a path of the API, ``None`` if it does not exist."""
        from databricks.sdk.errors import NotFound

        try:
            found = self.api.do("GET", path, query=dict(query) if query else None)
        except NotFound:
            return None
        return found if isinstance(found, Mapping) else None


def folder(path: object) -> str | None:
    """A workspace folder without its ``/Workspace`` prefix nor trailing slash, as the API may return it."""
    if not isinstance(path, str):
        return None
    path = path.rstrip("/")
    return path.removeprefix(WORKSPACE_PREFIX) if path.startswith(f"{WORKSPACE_PREFIX}/") else path
