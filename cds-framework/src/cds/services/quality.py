"""Quality service: checks the tables with DQX, from their contracts."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from cds.config import QualitySettings
from cds.data_context import DataProduct, Table
from cds.errors import ConfigurationError, ContractError
from cds.services.base import Service, TableResult
from cds.services.checks import CheckSet, build_checks, has_checks, validate_with_dqx
from cds.services.summary import (
    ERROR_RATE,
    PERCENT,
    SummaryRow,
    Threshold,
    create_statement,
    failures_query,
    insert_statement,
    summary_rows,
    summary_table,
)
from cds.services.tables import add_columns, column_definition, list_tables, overwrite
from cds.sla import percentage, table_sla
from cds.sql import ddl_type, quote_identifier, quote_name

if TYPE_CHECKING:
    from databricks.labs.dqx.engine import DQEngine
    from pyspark.sql import DataFrame, Observation

    from cds.context import ServiceContext

logger = logging.getLogger(__name__)


class CheckEngine(Protocol):
    """Applies DQX checks to a DataFrame, as ``DQEngine`` does."""

    def apply_checks_by_metadata(
        self, df: DataFrame, checks: list[dict[str, Any]]
    ) -> DataFrame | tuple[DataFrame, Observation]: ...


EngineFactory = Callable[[QualitySettings], CheckEngine]
Validator = Callable[[list[dict[str, Any]]], list[str]]


class QualityService(Service):
    """Applies DQX checks to the tables of a data product.

    The checks are inferred from the contracts and completed by the configured ones
    (see ``cds.services.checks``). Each row of a table gets the results of the checks,
    in the columns of ``QualitySettings`` (``_errors`` and ``_warnings`` by default),
    and the table is rewritten with them. The statistics of the table and of its checks,
    against the thresholds of its contract, are then appended to the summary table of
    its schema (``_quality_summary`` by default, see ``cds.services.summary``). A table
    with errors, or exceeding a threshold, is reported as a warning.

    Args:
        context: The service context.
        engine_factory: Creates the DQX engine for the settings of a table. By default
            a ``DQEngine`` using the workspace client of the context.
        validator: Validates the checks and returns the problems found. By default
            ``DQEngine.validate_checks``.
    """

    name = "quality"

    def __init__(
        self,
        context: ServiceContext,
        engine_factory: EngineFactory | None = None,
        validator: Validator | None = None,
    ) -> None:
        super().__init__(context)
        self._engine_factory = engine_factory or self._create_engine
        self._validator = validator or validate_with_dqx

    def prepare(self, data_product: DataProduct, tables: Sequence[Table]) -> None:
        schemas = {self.context.full_schema_name(table) for table in tables}
        self._existing = {name: list_tables(self.spark, name) for name in schemas}
        # The summary rows of the run share its start and its run_id parameter.
        self._checked_at = datetime.now(timezone.utc)
        self._run_id = self.context.runtime_configuration.get("run_id")
        self._summaries: set[str] = set()
        self._summary_lock = threading.Lock()
        if self._engine_factory == self._create_engine and any(
            table.settings(QualitySettings).enabled for table in tables
        ):
            # Create the workspace client once, before the tables are processed in parallel.
            _ = self.context.workspace_client

    def process(self, table: Table) -> TableResult:
        settings = table.settings(QualitySettings)
        if not settings.enabled:
            return TableResult.skipped(table.qualified_name, "quality checks disabled")
        full_name = self.context.full_table_name(table)
        if table.name.lower() not in self._existing[self.context.full_schema_name(table)]:
            return TableResult.skipped(table.qualified_name, f"{full_name} does not exist")
        check_set = build_checks(table, self.context)
        summary_name = summary_table(table, self.context)
        error_rate = _error_rate(table) if summary_name else None
        for note in check_set.unsupported:
            logger.warning("quality: not checked: %s", note)
        notes = f"; not checked: {'; '.join(check_set.unsupported)}" if check_set.unsupported else ""
        # A table with checks computed at run time gets the result columns even if none
        # remains, such as allowed values whose reference is empty: the dashboard and the
        # Genie space read them (see checked_tables). DQX then adds empty results.
        if not check_set.checks and not has_checks(table, self.context):
            return TableResult.skipped(table.qualified_name, f"no quality checks{notes}")
        problems = self._validator(check_set.checks)
        if problems:
            return TableResult.failure(table.qualified_name, f"invalid DQX checks: {'; '.join(problems)}")

        source = self.spark.table(quote_name(full_name))
        table_columns = list(source.columns)
        # DQX rejects input holding its result columns. They are all dropped, present or
        # not: the plan is resolved when the table is written, after the result columns
        # missing from the table were added to it.
        source = source.drop(*settings.result_columns)
        checked = self._engine_factory(settings).apply_checks_by_metadata(source, check_set.checks)
        if isinstance(checked, tuple):
            checked = checked[0]
        self._add_result_columns(full_name, checked, settings, table_columns)
        overwrite(self.spark, checked, full_name)

        total, errors, warnings = self._statistics(full_name, settings, checked.columns)
        message = (
            f"{len(check_set.checks)} check(s) on {total} row(s): {errors} row(s) with errors, {warnings} with warnings"
        )
        exceeded: list[str] = []
        if summary_name:
            rows = self._write_summary(
                table, summary_name, settings, check_set, error_rate, checked.columns, (total, errors, warnings)
            )
            exceeded = list(dict.fromkeys(row.check_name or row.metric for row in rows if row.passed is False))
            if exceeded:
                message += f"; {len(exceeded)} threshold(s) exceeded: {', '.join(exceeded)}"
        message += notes
        if errors or exceeded:
            return TableResult.warning(table.qualified_name, message)
        return TableResult.success(table.qualified_name, message)

    def _write_summary(
        self,
        table: Table,
        summary_name: str,
        settings: QualitySettings,
        check_set: CheckSet,
        error_rate: Threshold | None,
        columns: Sequence[str],
        statistics: tuple[int, int, int],
    ) -> list[SummaryRow]:
        """Appends the statistics of a checked table and of its checks to the summary table."""
        full_name = self.context.full_table_name(table)
        result_columns = [
            column for column in (settings.errors_column, settings.warnings_column) if column and column in columns
        ]
        query = failures_query(full_name, result_columns)
        failures = {row["name"]: row["failed_rows"] for row in self.spark.sql(query).collect()} if query else {}
        total, errors, warnings = statistics
        rows = summary_rows(
            checked_at=self._checked_at,
            run_id=self._run_id,
            data_product=table.data_product.qualified_name,
            table_name=table.name,
            checks=check_set.checks,
            thresholds=check_set.thresholds,
            error_rate=error_rate,
            total_rows=total,
            error_rows=errors,
            warning_rows=warnings,
            failures=failures,
        )
        with self._summary_lock:
            # Created once per run: the tables of a schema are checked in parallel.
            if summary_name not in self._summaries:
                self.spark.sql(create_statement(summary_name))
                self._summaries.add(summary_name)
        self.spark.sql(insert_statement(summary_name, rows))
        return rows

    def _add_result_columns(
        self, full_name: str, checked: DataFrame, settings: QualitySettings, table_columns: list[str]
    ) -> None:
        existing = {column.lower() for column in table_columns}
        definitions = [
            column_definition(column, ddl_type(checked.schema[column].dataType.jsonValue()))
            for column in settings.result_columns
            if column in checked.columns and column.lower() not in existing
        ]
        add_columns(self.spark, full_name, definitions)

    def _statistics(self, full_name: str, settings: QualitySettings, columns: list[str]) -> tuple[int, int, int]:
        counts = ["count(*) AS total"]
        for alias, column in (("errors", settings.errors_column), ("warnings", settings.warnings_column)):
            if column in columns:
                counts.append(f"count_if(size({quote_identifier(column)}) > 0) AS {alias}")
            else:
                counts.append(f"0 AS {alias}")
        row = self.spark.sql(f"SELECT {', '.join(counts)} FROM {quote_name(full_name)}").collect()[0]
        return row["total"], row["errors"], row["warnings"]

    def _create_engine(self, settings: QualitySettings) -> DQEngine:
        try:
            from databricks.labs.dqx.config import ExtraParams
            from databricks.labs.dqx.engine import DQEngine
        except ImportError as exc:
            raise ConfigurationError("DQX is required by the quality service: install cds-framework[dqx].") from exc
        names = {
            "errors": settings.errors_column,
            "warnings": settings.warnings_column,
            "info": settings.info_column,
        }
        return DQEngine(
            self.context.workspace_client,
            spark=self.spark,
            extra_params=ExtraParams(result_column_names={key: value for key, value in names.items() if value}),
        )


def _error_rate(table: Table) -> Threshold | None:
    """The ``errorRate`` SLA of a table, as a threshold: at most this percentage of rows with errors.

    Raises:
        ContractError: If the SLA is invalid.
    """
    try:
        found = table_sla(table, ERROR_RATE)
        if found is None:
            return None
        return Threshold("mustBeLessOrEqualTo", percentage(found[0]), unit=PERCENT, metric=ERROR_RATE)
    except ContractError as exc:
        raise ContractError(f"{table.qualified_name}: {exc}") from None
