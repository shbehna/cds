"""Generates the deployment manifest of the data products of the demo, read by the Pulumi program.

The Pulumi program (cds-demo/deploy) cannot load the data products: their configurations
import PySpark, which its environment does not install. The manifest holds what it needs
to know of the contracts. For the whole demo, whose data products share a catalog per
environment and the governed tags of the account:

- ``classification``: the key and the allowed values of the classification tag of the
  columns, which the deployment declares as a governed tag and masks. The data products
  must share them: the masking policy applies to the whole catalog;
- ``domains``: the Discover domains of the tables and their parent domains, parents
  first: the governed tag of each domain, and that of its parent (``None`` for a
  top-level domain), which the deployment creates with the domains.

And for each data product (``data_products``), which the deployment runs in a job of its own:

- ``data_product``: its qualified name, ``<group>.<data product>``, and ``title``: the name
  of its ODPS document;
- ``schedule``: the schedule of its job, from the ``frequency`` SLA of its tables, as a
  cron expression and in the Quartz syntax of Databricks;
- ``alerts``: the SQL alerts checking the thresholds of the quality of its tables and
  their ``latency`` SLA, with their schedules. The query of an alert returns a row per
  measure, whose ``exceeded`` column is 1 when it breaks its threshold: the alert
  triggers when one of them does. The ``quality`` alert reads the quality summary of the
  tables, which the quality service writes; the ``latency`` alert, their last change;
- ``recipients``: the addresses of the email support channels of its contracts, which
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

from cds import DataContext, DataProduct, MetadataSettings, ServiceContext, Table
from cds.config import resolved
from cds.errors import ConfigurationError
from cds.odcs import SlaProperty
from cds.services.checks import build_checks, checked_tables
from cds.services.summary import latest_thresholds_query, summary_table, summary_tables
from cds.services.tags import DOMAIN_SEPARATOR, discover_domain
from cds.sla import cron_schedule, duration, quartz_cron, support_emails, table_sla
from cds.sql import quote_identifier, quote_name, string_literal

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
MANIFEST = Path(__file__).with_name("manifest.json")
# Placeholder of the environment in the queries, replaced by the deployment.
ENVIRONMENT = "${environment}"
# Environment id naming the tables, then replaced by the placeholder.
_SENTINEL = "cds_manifest_environment"
# Column of the alert queries: 1 for a table breaking its SLA, 0 otherwise.
EXCEEDED = "exceeded"


def data_products(data_context: DataContext) -> list[DataProduct]:
    """The data products of the demo, by qualified name."""
    return sorted(data_context.data_products, key=lambda data_product: data_product.qualified_name)


def classification(data_context: DataContext) -> dict[str, Any]:
    """The classification tag of the columns of the data products: its key and allowed values.

    Raises:
        ConfigurationError: If the classifications are not declared, or differ between
            data products or tables.
    """
    products = data_products(data_context)
    if not products:
        raise ConfigurationError("No data product: the manifest needs one.")
    first = products[0]
    settings = first.settings(MetadataSettings)
    if not settings.classifications:
        raise ConfigurationError(f"{first.qualified_name}: MetadataSettings.classifications is not set.")
    expected = (settings.classification_tag, settings.classifications)
    elements: list[DataProduct | Table] = [*products, *(table for product in products for table in product.tables)]
    for element in elements:
        element_settings = element.settings(MetadataSettings)
        if (element_settings.classification_tag, element_settings.classifications) != expected:
            raise ConfigurationError(
                f"{element.qualified_name}: the classification tag and values must be those of "
                f"{first.qualified_name}: the masking applies to the whole catalog."
            )
    return {"tag": resolved(settings.classification_tag), "values": list(settings.classifications)}


def domains(data_context: DataContext) -> list[dict[str, str | None]]:
    """The Discover domains of the tables and their parents, parents first: their tag and that of their parent.

    A subdomain is created under its parent: ``Économie/Registraire des entreprises du
    Québec`` under ``Économie``.
    """
    found: set[str] = set()
    for table in data_context.tables:
        tag = discover_domain(table)
        levels = tag.split(DOMAIN_SEPARATOR) if tag else []
        found.update(DOMAIN_SEPARATOR.join(levels[:depth]) for depth in range(1, len(levels) + 1))
    return [
        {"tag": tag, "parent": tag.rpartition(DOMAIN_SEPARATOR)[0] or None}
        for tag in sorted(found, key=lambda tag: (tag.count(DOMAIN_SEPARATOR), tag))
    ]


def _tables(data_context: DataContext, data_product: str) -> list[Table]:
    return sorted(data_context.get_data_product(data_product).tables, key=lambda table: table.name)


def _scheduled(table: Table, name: str) -> tuple[SlaProperty, str | None, str] | None:
    """The SLA property of a table, the column it designates, and its cron schedule.

    Raises:
        ConfigurationError: If the property has no cron schedule.
    """
    found = table_sla(table, name)
    if found is None:
        return None
    prop, column = found
    schedule = cron_schedule(prop)
    if schedule is None:
        raise ConfigurationError(f"{table.qualified_name}: the {name} SLA needs a cron schedule (scheduler: cron).")
    return prop, column, schedule


def _common_schedule(data_product: str, name: str, schedules: Mapping[str, str]) -> dict[str, str]:
    """The schedule shared by the SLA of the tables, as a cron expression and in the Quartz syntax.

    Raises:
        ConfigurationError: If the tables have different schedules.
    """
    if len(set(schedules.values())) > 1:
        details = ", ".join(f"{table} ({cron})" for table, cron in sorted(schedules.items()))
        raise ConfigurationError(f"{data_product}: the tables must share the schedule of their {name} SLA: {details}.")
    cron = next(iter(schedules.values()))
    return {"cron": cron, "quartz": quartz_cron(cron)}


def _context(data_context: DataContext) -> ServiceContext:
    return ServiceContext(spark=None, environment=_SENTINEL, data_context=data_context)


def _name(full_name: str) -> str:
    """A name of the naming conventions, with the placeholder of the environment."""
    return full_name.replace(_SENTINEL, ENVIRONMENT)


def schedule(data_context: DataContext, data_product: str) -> dict[str, str] | None:
    """The schedule of the job of a data product: the ``frequency`` SLA of its tables, ``None`` without it.

    Raises:
        ConfigurationError: If the tables have different frequencies, or one without cron schedule.
    """
    schedules = {}
    for table in _tables(data_context, data_product):
        found = _scheduled(table, "frequency")
        if found is not None:
            schedules[table.name] = found[2]
    return _common_schedule(data_product, "frequency", schedules) if schedules else None


def quality_alert(data_context: DataContext, data_product: str) -> dict[str, Any] | None:
    """The alert on the thresholds of the quality of a data product, read from its quality summary.

    The quality service appends the measures of each checked table to the summary table
    of its schema, against the thresholds of its contract (``cds.services.summary``): its
    ``errorRate`` SLA, and the ``quality`` rules with thresholds. The query returns the
    latest measures of each table with a threshold, exceeded first, instead of scanning the
    tables. The alert runs on the schedule of the ``errorRate`` SLA, after the loads.

    Only the tables with the SLA are read, on its schedule: a table whose ``quality``
    rules have thresholds needs the SLA, else they would not be alerted.

    Returns:
        The alert; ``None`` if no checked table with a summary has the ``errorRate`` SLA.

    Raises:
        ConfigurationError: If the SLA of the tables have different schedules, or one without
            cron schedule, or a table with thresholds has no ``errorRate`` SLA.
    """
    context = _context(data_context)
    tables = [
        table
        for table in checked_tables(data_context.get_data_product(data_product), context)
        if summary_table(table, context)
    ]
    schedules = {}
    for table in tables:
        found = _scheduled(table, "errorRate")
        if found is not None:
            schedules[table.name] = found[2]
    unscheduled = [
        table.name
        for table in tables
        if table.name not in schedules
        and build_checks(table, context, include_configured=False, read_values=False).thresholds
    ]
    if unscheduled:
        raise ConfigurationError(
            f"{data_product}: the thresholds of {', '.join(unscheduled)} would not be alerted: give the "
            "table an errorRate SLA with a cron schedule, which schedules the quality alert."
        )
    alerted = [table for table in tables if table.name in schedules]
    if not alerted:
        return None
    summaries = {_name(name): names for name, names in summary_tables(alerted, context).items()}
    query = (
        "SELECT table_name, measure, criticality, round(actual, 2) AS actual, unit, operator, threshold,\n"
        f"  threshold_high, checked_at, CASE WHEN passed THEN 0 ELSE 1 END AS {EXCEEDED}\n"
        f"FROM (\n{latest_thresholds_query(summaries, data_product)}\n) AS thresholds\n"
        f"ORDER BY {EXCEEDED} DESC, table_name, measure"
    )
    return {
        "name": "quality",
        "title": "quality",
        "sla": "errorRate",
        **_common_schedule(data_product, "errorRate", schedules),
        "query": query,
    }


def latency_alert(data_context: DataContext, data_product: str) -> dict[str, Any] | None:
    """The alert on the ``latency`` SLA of a data product: the age of the last change of its tables.

    The last change of a table is the ``last_altered`` time of the information schema,
    which the metadata step of the job updates: it runs after the load, and not when the
    load fails. When the element of the SLA is a column, it is the latest value of the
    column instead. ``None`` if no table has the SLA.
    """
    context = _context(data_context)
    selects = []
    schedules = {}
    for table in _tables(data_context, data_product):
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
                # Unity Catalog stores the names in lowercase.
                f"WHERE table_schema = {string_literal(schema.lower())} "
                f"AND table_name = {string_literal(table.name.lower())}"
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
        **_common_schedule(data_product, "latency", schedules),
        "query": query,
    }


def recipients(data_context: DataContext, data_product: str) -> list[str]:
    """The addresses of the email support channels of the contracts of a data product."""
    contracts = data_context.get_data_product(data_product).contracts
    addresses = (
        address for contract in contracts if contract.document for address in support_emails(contract.document)
    )
    return list(dict.fromkeys(addresses))


def data_product_manifest(data_context: DataContext, data_product: str) -> dict[str, Any]:
    """What the deployment needs to know of a data product: its job, its alerts and their recipients."""
    document = data_context.get_data_product(data_product).document
    alerts = [
        alert
        for alert in (quality_alert(data_context, data_product), latency_alert(data_context, data_product))
        if alert
    ]
    return {
        "data_product": data_product,
        "title": document.name if document else None,
        "schedule": schedule(data_context, data_product),
        "alerts": alerts,
        "recipients": recipients(data_context, data_product),
    }


def build(data_context: DataContext) -> dict[str, Any]:
    """The manifest of the data products."""
    return {
        "classification": classification(data_context),
        "domains": domains(data_context),
        "data_products": [
            data_product_manifest(data_context, data_product.qualified_name)
            for data_product in data_products(data_context)
        ],
    }


def render(data_context: DataContext | None = None) -> str:
    """Returns the serialized manifest, as committed."""
    data_context = data_context or DataContext.discover(PRODUCTS)
    return json.dumps(build(data_context), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    MANIFEST.write_text(render(), encoding="utf-8", newline="\n")
    print(f"Wrote {MANIFEST}")
