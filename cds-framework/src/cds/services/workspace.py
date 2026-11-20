"""Base class of the services creating a workspace object of a data product, such as its Genie space."""

from __future__ import annotations

import contextlib
import json
import logging
from abc import abstractmethod
from collections.abc import Iterable, Mapping, Sequence
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
TAG_ASSIGNMENTS_PATH = "/api/2.0/entity-tag-assignments"


class DataProductService(Service):
    """A service processing a data product as a whole, not its tables.

    It reports a single result, named after the data product. The workspace object it
    creates is recorded in a property of the schema of the data product, which the next
    runs read to update it. The object joins the Discover domains of its tables (see
    ``tag_domains``).
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

    def schema_properties(self, schema: str) -> dict[str, str]:
        """The properties of a schema, such as the ids the services recorded in it."""
        found = self.api.do("GET", f"{SCHEMAS_PATH}/{quote(schema, safe='')}")
        properties = (found.get("properties") if isinstance(found, Mapping) else None) or {}
        return {str(key): str(value) for key, value in properties.items() if value is not None}

    def set_schema_property(self, schema: str, key: str, value: str) -> None:
        """Sets a property of a schema."""
        self.spark.sql(
            f"ALTER SCHEMA {quote_name(schema)} SET DBPROPERTIES ({string_literal(key)} = {string_literal(value)})"
        )

    def record_id(self, schema: str, key: str, value: str, delete_path: str) -> None:
        """Records the id of a new object in a property of a schema.

        If it cannot be recorded, the object is deleted with ``DELETE <delete_path>``, and
        the error raised: the next run would not find the object, and would create another.
        """
        try:
            self.set_schema_property(schema, key, value)
        except Exception:
            logger.error("%s: cannot record the %s %s in %s: deleting it", self.name, self.object_name, value, schema)
            try:
                self.api.do("DELETE", delete_path)
            except Exception:
                logger.exception("%s: cannot delete the %s %s", self.name, self.object_name, value)
            raise

    def tag_domains(
        self,
        entity_type: str,
        entity_id: str,
        domains: Sequence[str],
        schema: str,
        properties: Mapping[str, str],
        key: str,
    ) -> str | None:
        """Tags a workspace object with the governed tags of its Discover domains.

        A dashboard or a Genie space joins a Discover domain with its governed tag, as a
        table does. The keys of the tags the service applies are recorded in the ``key``
        property of the schema, before they are applied: the next runs remove those no
        longer expected, even after a run that failed halfway. The other tags of the object,
        such as a domain added in the Discover page, are left untouched, even when the
        contracts later expect it.

        Args:
            entity_type: The type of the object for the tag API: ``dashboards`` or ``geniespaces``.
            entity_id: The id of the object.
            domains: The governed tags of its Discover domains (see
                ``cds.services.tags.discover_domains``).
            schema: The schema recording the applied keys.
            properties: The properties of the schema.
            key: The property recording the applied keys.

        Returns:
            The problem, if the object could not be tagged, such as a missing ``ASSIGN``
            permission on a governed tag; ``None`` otherwise.
        """
        recorded = recorded_keys(properties.get(key))
        if not domains and not recorded:
            return None
        from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists

        expected = set(domains)
        path = f"{TAG_ASSIGNMENTS_PATH}/{quote(entity_type, safe='')}/{quote(entity_id, safe='')}/tags"
        try:
            actual = self._tag_keys(path)
            # A tag the object already has is the service's only if it applied it.
            to_add = sorted(expected - actual)
            to_remove = sorted((recorded - expected) & actual)
            pending = recorded | set(to_add)
            if pending != recorded:
                self._record_keys(schema, key, pending)
                recorded = pending
            for tag in to_add:
                # Assigned since it was listed, by another run or a user.
                with contextlib.suppress(AlreadyExists, ResourceAlreadyExists):
                    self.api.do(
                        "POST",
                        TAG_ASSIGNMENTS_PATH,
                        body={"entity_type": entity_type, "entity_id": entity_id, "tag_key": tag},
                    )
            for tag in to_remove:
                # Not found would mean it was not removed: the error keeps it recorded.
                self.api.do("DELETE", f"{path}/{quote(tag, safe='')}")
            applied = (recorded & expected & actual) | set(to_add)
            if applied != recorded:
                self._record_keys(schema, key, applied)
        except Exception as exc:
            logger.exception(
                "%s: cannot tag the %s %s with its Discover domains", self.name, self.object_name, entity_id
            )
            return f"cannot tag it with its Discover domain(s): {type(exc).__name__}: {exc}"
        return None

    def _record_keys(self, schema: str, key: str, keys: Iterable[str]) -> None:
        """Records the keys of the tags applied by the service in a property of a schema."""
        self.set_schema_property(schema, key, json.dumps(sorted(keys), ensure_ascii=False))

    def _tag_keys(self, path: str) -> set[str]:
        """The keys of the tags of a workspace object, from all the pages of the tag API."""
        keys: set[str] = set()
        query: dict[str, str] = {}
        while True:
            page = self.api.do("GET", path, query=dict(query) if query else None) or {}
            keys.update(str(item["tag_key"]) for item in page.get("tag_assignments") or [] if "tag_key" in item)
            token = page.get("next_page_token")
            if not token:
                return keys
            query["page_token"] = str(token)

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


def recorded_keys(value: str | None) -> set[str]:
    """The tag keys recorded in a schema property, as a JSON list; none if it is missing or unreadable."""
    try:
        recorded = json.loads(value or "[]")
    except ValueError:
        return set()
    return {key for key in recorded if isinstance(key, str)} if isinstance(recorded, list) else set()
