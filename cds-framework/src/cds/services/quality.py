"""Quality service: checks the tables with DQX, from their contracts."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, Protocol

from cds.config import QualitySettings
from cds.data_context import DataProduct, Table
from cds.errors import ConfigurationError
from cds.services.base import Service, TableResult
from cds.services.checks import build_checks, validate_with_dqx
from cds.services.tables import add_columns, column_definition, list_tables, overwrite
from cds.sql import ddl_type, quote_identifier, quote_name

if TYPE_CHECKING:
    from databricks.labs.dqx.engine import DQEngine
    from pyspark.sql import DataFrame, Observation

    from cds.context import ServiceContext


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
    and the table is rewritten with them.

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
        notes = f"; not checked: {'; '.join(check_set.unsupported)}" if check_set.unsupported else ""
        if not check_set.checks:
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
            f"{len(check_set.checks)} check(s) on {total} row(s): {errors} row(s) with errors, "
            f"{warnings} with warnings{notes}"
        )
        if errors:
            return TableResult.warning(table.qualified_name, message)
        return TableResult.success(table.qualified_name, message)

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
