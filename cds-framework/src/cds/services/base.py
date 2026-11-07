"""Base class of the services, and the reporting of their results."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, ClassVar

from cds.errors import ConfigurationError, ServiceError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from cds.context import ServiceContext
    from cds.data_context import DataProduct, Table

logger = logging.getLogger(__name__)

DEFAULT_MAX_WORKERS = 4


class Status(str, Enum):
    """Outcome of the processing of a table."""

    SUCCESS = "success"
    SKIPPED = "skipped"
    WARNING = "warning"
    ERROR = "error"


_SYMBOLS = {Status.SUCCESS: "+", Status.SKIPPED: "-", Status.WARNING: "!", Status.ERROR: "x"}


@dataclass(frozen=True)
class TableResult:
    """Outcome of the processing of a table by a service."""

    table: str
    status: Status
    message: str
    error: BaseException | None = field(default=None, compare=False)

    @classmethod
    def success(cls, table: str, message: str) -> TableResult:
        return cls(table, Status.SUCCESS, message)

    @classmethod
    def skipped(cls, table: str, message: str) -> TableResult:
        return cls(table, Status.SKIPPED, message)

    @classmethod
    def warning(cls, table: str, message: str) -> TableResult:
        return cls(table, Status.WARNING, message)

    @classmethod
    def failure(cls, table: str, message: str, error: BaseException | None = None) -> TableResult:
        return cls(table, Status.ERROR, message, error)


@dataclass
class ServiceReport:
    """Results of a service run on a data product."""

    service: str
    data_product: str
    results: list[TableResult]

    def with_status(self, status: Status) -> list[TableResult]:
        return [result for result in self.results if result.status is status]

    @property
    def has_errors(self) -> bool:
        return any(result.status is Status.ERROR for result in self.results)

    def summary(self) -> str:
        """Returns a human readable summary of the run."""
        counts = ", ".join(f"{len(self.with_status(status))} {status.value}" for status in Status)
        lines = [f"{self.service} service on {self.data_product}: {len(self.results)} table(s), {counts}"]
        lines += [f"  {_SYMBOLS[r.status]} {r.table}: {r.message}" for r in self.results]
        return "\n".join(lines)

    def log(self, target: logging.Logger | None = None) -> None:
        target = target or logger
        level = logging.ERROR if self.has_errors else logging.INFO
        target.log(level, "%s", self.summary())

    def raise_for_errors(self) -> None:
        """Raises a ``ServiceError`` if at least one table is in error."""
        errors = self.with_status(Status.ERROR)
        if errors:
            details = "; ".join(f"{result.table}: {result.message}" for result in errors)
            raise ServiceError(
                f"The {self.service} service failed for {len(errors)} table(s) of {self.data_product}: {details}"
            )


class Service(ABC):
    """A service processing the tables of a data product.

    Subclasses implement ``process`` for a single table, and may override ``prepare``
    to run work once, before the tables are processed in parallel.
    """

    name: ClassVar[str]

    def __init__(self, context: ServiceContext) -> None:
        self.context = context

    @property
    def spark(self) -> SparkSession:
        return self.context.spark

    def run(
        self,
        data_product: DataProduct,
        tables: Iterable[str] | None = None,
        max_workers: int | None = None,
    ) -> ServiceReport:
        """Processes the tables of a data product.

        Args:
            data_product: The data product.
            tables: Names of the tables to process; all the tables by default.
            max_workers: Number of tables processed in parallel.

        Returns:
            The results of the run. Errors are reported, not raised: see
            ``ServiceReport.raise_for_errors``.
        """
        selected = select_tables(data_product, tables)
        logger.info(
            "%s service: %s table(s) of %s in environment %s",
            self.name,
            len(selected),
            data_product.qualified_name,
            self.context.environment,
        )
        self.prepare(data_product, selected)
        workers = max(1, min(max_workers or DEFAULT_MAX_WORKERS, len(selected) or 1))
        if workers == 1:
            results = [self._process(table) for table in selected]
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"cds-{self.name}") as pool:
                results = list(pool.map(self._process, selected))
        return ServiceReport(self.name, data_product.qualified_name, results)

    def prepare(self, data_product: DataProduct, tables: Sequence[Table]) -> None:  # noqa: B027
        """Runs once before the tables are processed. Does nothing by default."""

    @abstractmethod
    def process(self, table: Table) -> TableResult:
        """Processes a table and returns its result."""

    def _process(self, table: Table) -> TableResult:
        logger.info("%s: processing %s", self.name, table.qualified_name)
        try:
            result = self.process(table)
        except Exception as exc:
            logger.exception("%s: %s failed", self.name, table.qualified_name)
            return TableResult.failure(table.qualified_name, f"{type(exc).__name__}: {exc}", exc)
        logger.info("%s: %s %s - %s", self.name, table.qualified_name, result.status.value, result.message)
        return result


def select_tables(data_product: DataProduct, names: Iterable[str] | None) -> list[Table]:
    """Returns the tables of a data product, optionally restricted to some names.

    Raises:
        ConfigurationError: If a name is not a table of the data product.
    """
    tables = data_product.tables
    if not names:
        return tables
    wanted = {name.lower() for name in names}
    unknown = wanted - {table.name.lower() for table in tables}
    if unknown:
        raise ConfigurationError(f"Unknown table(s) {', '.join(sorted(unknown))} in {data_product.qualified_name}.")
    return [table for table in tables if table.name.lower() in wanted]
