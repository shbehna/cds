"""Entry point running a service, from a Databricks notebook, a job or a script."""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

from cds.context import ServiceContext
from cds.errors import ConfigurationError
from cds.runtime import DBUtils
from cds.services import SERVICES, Service, ServiceReport

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Runtime parameters common to all the services, declared as notebook widgets.
PARAMETERS = {
    "environment": "Environment id, e.g. nprod or prod.",
    "roots": "Comma-separated root folders of the data products.",
    "data_product": "Data product to process: <group>.<data_product>.",
    "tables": "Optional comma-separated tables to process; all by default.",
    "max_workers": "Optional number of tables processed in parallel.",
}


def run_service(
    service: str | type[Service],
    *,
    spark: SparkSession | None = None,
    dbutils: DBUtils | None = None,
    roots: str | Path | Iterable[str | Path] | None = None,
    **parameters: object,
) -> ServiceReport:
    """Runs a service on a data product and raises if a table is in error.

    The runtime parameters (see ``PARAMETERS``) are read from the keyword arguments,
    then from the notebook widgets and job parameters, then from the ``CDS_*``
    environment variables.

    Args:
        service: Name of the service (``schema``, ``metadata``, ``transformation``,
            ``quality``, ``scd`` or ``snapshot``) or its class.
        spark: The Spark session; the active session by default.
        dbutils: The Databricks utilities of the notebook, to read its widgets.
        roots: The root folders of the data products, instead of the ``roots`` parameter.
        **parameters: Runtime parameters, taking precedence over the other sources.

    Returns:
        The report of the run.

    Raises:
        ServiceError: If at least one table is in error.
        ConfigurationError: If the parameters or the project are invalid.
    """
    configure_logging()
    service_class = get_service(service)
    if dbutils is not None:
        declare_widgets(dbutils)
    context = ServiceContext.create(spark=spark, dbutils=dbutils, roots=roots, parameters=parameters)
    runtime = context.runtime_configuration
    report = service_class(context).run(
        context.get_data_product(),
        tables=runtime.get_list("tables") or None,
        max_workers=runtime.get_int("max_workers"),
    )
    report.log()
    report.raise_for_errors()
    return report


def get_service(service: str | type[Service]) -> type[Service]:
    """Returns a service class from its name."""
    if isinstance(service, type):
        return service
    try:
        return SERVICES[service]
    except KeyError:
        raise ConfigurationError(f"Unknown service {service!r}, expected one of {', '.join(SERVICES)}.") from None


def declare_widgets(dbutils: DBUtils) -> None:
    """Declares the common parameters as text widgets, to run a notebook interactively.

    Values passed as job parameters take precedence over the empty default.
    """
    for name in PARAMETERS:
        try:
            dbutils.widgets.text(name, "")
        except Exception as exc:
            logger.debug("Cannot declare the widget %s: %s", name, exc)


def configure_logging(level: int = logging.INFO) -> None:
    """Sends the logs of the framework to the standard output, once."""
    framework = logging.getLogger("cds")
    if not any(getattr(handler, "_cds", False) for handler in framework.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        handler._cds = True  # type: ignore[attr-defined]
        framework.addHandler(handler)
        framework.propagate = False
    framework.setLevel(level)
