"""Generates the deployment manifest of the REQ data product, read by the Pulumi program.

The Pulumi program (cds-demo/deploy) cannot load the data products: their configurations
import PySpark, which its environment does not install. The manifest holds what it needs
to know of the contracts:

- ``classification``: the key and the allowed values of the classification tag of the
  columns, which the deployment declares as a governed tag and masks;
- ``schedule``: the schedule of the job, from the ``frequency`` SLA of the tables, as a
  cron expression and in the Quartz syntax of Databricks;
- ``alerts``: the SQL alerts checking the ``errorRate`` and ``latency`` SLA of the tables,
  with their schedules. The query of an alert returns a row per table, whose ``exceeded``
  column is 1 when the table breaks its SLA: the alert triggers when one of them does;
- ``recipients``: the addresses of the email support channels of the contracts, which
  receive the alerts.

The queries name the tables in the catalog and schema of the naming conventions, with the
placeholder ``${environment}`` for the environment, replaced by the deployment.

The generated file is committed; regenerate it, from the root of the repository, after
changing the contracts or their configurations:

    uv run python cds-demo/manifest/manifest.py
"""

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cds import DataContext, MetadataSettings, QualitySettings, ServiceContext, Table
from cds.config import resolved
from cds.errors import ConfigurationError
from cds.odcs import SlaProperty
from cds.sla import duration, percentage, quartz_cron, support_emails, table_sla
from cds.sql import quote_identifier, quote_name, string_literal

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
MANIFEST = Path(__file__).with_name("req_manifest.json")
DATA_PRODUCT = "donnees_qc.req"
# Placeholder of the environment in the queries, replaced by the deployment.
ENVIRONMENT = "${environment}"
# Environment id naming the tables, then replaced by the placeholder.
_SENTINEL = "cds_manifest_environment"
# Column of the alert queries: 1 for a table breaking its SLA, 0 otherwise.
EXCEEDED = "exceeded"


def classification(data_context: DataContext) -> dict[str, Any]:
    """The classification tag of the columns of the data product: its key and allowed values.

    Raises:
        ConfigurationError: If the classifications are not declared, or differ between tables.
    """
    data_product = data_context.get_data_product(DATA_PRODUCT)
    settings = data_product.settings(MetadataSettings)
    if not settings.classifications:
        raise ConfigurationError(f"{DATA_PRODUCT}: MetadataSettings.classifications is not set.")
    for table in data_product.tables:
        table_settings = table.settings(MetadataSettings)
        if (table_settings.classification_tag, table_settings.classifications) != (
            settings.classification_tag,
            settings.classifications,
        ):
            raise ConfigurationError(
                f"{table.qualified_name}: the classification tag and values must be those of {DATA_PRODUCT}."
            )
    return {"tag": resolved(settings.classification_tag), "values": list(settings.classifications)}


def _tables(data_context: DataContext) -> list[Table]:
    return sorted(data_context.get_data_product(DATA_PRODUCT).tables, key=lambda table: table.name)


def _scheduled(table: Table, name: str) -> tuple[SlaProperty, str | None, str] | None:
    """The SLA property of a table, the column it designates, and its cron schedule.

    Raises:
        ConfigurationError: If the property has no cron schedule.
    """
    found = table_sla(table, name)
    if found is None:
        return None
    prop, column = found
    if (prop.scheduler or "").lower() != "cron" or not prop.schedule:
        raise ConfigurationError(f"{table.qualified_name}: the {name} SLA needs a cron schedule (scheduler: cron).")
    return prop, column, prop.schedule


def _common_schedule(name: str, schedules: Mapping[str, str]) -> dict[str, str]:
    """The schedule shared by the SLA of the tables, as a cron expression and in the Quartz syntax.

    Raises:
        ConfigurationError: If the tables have different schedules.
    """
    if len(set(schedules.values())) > 1:
        details = ", ".join(f"{table} ({cron})" for table, cron in sorted(schedules.items()))
        raise ConfigurationError(f"{DATA_PRODUCT}: the tables must share the schedule of their {name} SLA: {details}.")
    cron = next(iter(schedules.values()))
    return {"cron": cron, "quartz": quartz_cron(cron)}


def _context(data_context: DataContext) -> ServiceContext:
    return ServiceContext(spark=None, environment=_SENTINEL, data_context=data_context)


def _name(full_name: str) -> str:
    """A name of the naming conventions, with the placeholder of the environment."""
    return full_name.replace(_SENTINEL, ENVIRONMENT)


def schedule(data_context: DataContext) -> dict[str, str] | None:
    """The schedule of the job: the ``frequency`` SLA of the tables, ``None`` without it.

    Raises:
        ConfigurationError: If the tables have different frequencies, or one without cron schedule.
    """
    schedules = {}
    for table in _tables(data_context):
        found = _scheduled(table, "frequency")
        if found is not None:
            schedules[table.name] = found[2]
    return _common_schedule("frequency", schedules) if schedules else None


def error_rate_alert(data_context: DataContext) -> dict[str, Any] | None:
    """The alert on the ``errorRate`` SLA: the share of the rows with a quality error, in percent.

    Only the tables checked by the quality service, which writes the errors of each row,
    are included. ``None`` if no table has the SLA.
    """
    context = _context(data_context)
    selects = []
    schedules = {}
    for table in _tables(data_context):
        found = _scheduled(table, "errorRate") if table.settings(QualitySettings).enabled else None
        if found is None:
            continue
        prop, _, schedules[table.name] = found
        errors = quote_identifier(resolved(table.settings(QualitySettings).errors_column))
        selects.append(
            f"  SELECT {string_literal(table.name)} AS table_name, {percentage(prop)!r} AS threshold,\n"
            f"    try_divide(100 * count_if(coalesce(size({errors}), 0) > 0), count(*)) AS error_rate\n"
            f"  FROM {quote_name(_name(context.full_table_name(table)))}"
        )
    if not selects:
        return None
    query = (
        "SELECT table_name, round(error_rate, 2) AS error_rate, threshold,\n"
        f"  CASE WHEN error_rate > threshold THEN 1 ELSE 0 END AS {EXCEEDED}\n"
        "FROM (\n" + "\n  UNION ALL\n".join(selects) + "\n) AS rates\nORDER BY table_name"
    )
    return {
        "name": "error-rate",
        "title": "error rate",
        "sla": "errorRate",
        **_common_schedule("errorRate", schedules),
        "query": query,
    }


def latency_alert(data_context: DataContext) -> dict[str, Any] | None:
    """The alert on the ``latency`` SLA: the age of the last change of the tables.

    The last change of a table is the ``last_altered`` time of the information schema,
    which its load and the metadata step of the job update. When the element of the SLA is
    a column, it is the latest value of the column instead. ``None`` if no table has the SLA.
    """
    context = _context(data_context)
    selects = []
    schedules = {}
    for table in _tables(data_context):
        found = _scheduled(table, "latency")
        if found is None:
            continue
        prop, column, schedules[table.name] = found
        latency = duration(prop)
        if column:
            updated = (
                f"SELECT CAST(max({quote_identifier(column)}) AS TIMESTAMP) AS updated_at "
                f"FROM {quote_name(_name(context.full_table_name(table)))}"
            )
        else:
            catalog, schema = _name(context.full_schema_name(table)).split(".")
            tables = f"{quote_identifier(catalog)}.`information_schema`.`tables`"
            updated = (
                f"SELECT max(last_altered) AS updated_at FROM {tables} "
                f"WHERE table_schema = {string_literal(schema)} AND table_name = {string_literal(table.name.lower())}"
            )
        selects.append(
            f"  SELECT {string_literal(table.name)} AS table_name, {string_literal(str(latency))} AS latency, "
            "updated_at,\n"
            f"    CASE WHEN updated_at >= current_timestamp() - {latency.sql()} THEN 0 ELSE 1 END AS {EXCEEDED}\n"
            f"  FROM ({updated}) AS t"
        )
    if not selects:
        return None
    query = "SELECT * FROM (\n" + "\n  UNION ALL\n".join(selects) + "\n) AS freshness\nORDER BY table_name"
    return {
        "name": "latency",
        "title": "latency",
        "sla": "latency",
        **_common_schedule("latency", schedules),
        "query": query,
    }


def recipients(data_context: DataContext) -> list[str]:
    """The addresses of the email support channels of the contracts of the data product."""
    contracts = data_context.get_data_product(DATA_PRODUCT).contracts
    addresses = (
        address for contract in contracts if contract.document for address in support_emails(contract.document)
    )
    return list(dict.fromkeys(addresses))


def build(data_context: DataContext) -> dict[str, Any]:
    """The manifest of the data product."""
    alerts = [alert for alert in (error_rate_alert(data_context), latency_alert(data_context)) if alert]
    return {
        "data_product": DATA_PRODUCT,
        "classification": classification(data_context),
        "schedule": schedule(data_context),
        "alerts": alerts,
        "recipients": recipients(data_context),
    }


def render(data_context: DataContext | None = None) -> str:
    """Returns the serialized manifest, as committed."""
    data_context = data_context or DataContext.discover(PRODUCTS)
    return json.dumps(build(data_context), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    MANIFEST.write_text(render(), encoding="utf-8", newline="\n")
    print(f"Wrote {MANIFEST}")
