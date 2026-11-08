"""Quality checks of the codes of the register against its value domains (domaine_valeur)."""

import logging
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from pyspark.sql import SparkSession

from cds import DataProduct, ServiceContext, literal_values

logger = logging.getLogger(__name__)


def domain_checks(
    spark: SparkSession,
    service_context: ServiceContext,
    data_product: DataProduct,
    columns: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Returns DQX checks verifying that code columns belong to their value domain.

    The codes are read at run time from the domaine_valeur table of the data product,
    which the transformation service loads before the quality service runs.

    Args:
        spark: The Spark session.
        service_context: The service context, to resolve the name of the table.
        data_product: The data product of the checked table.
        columns: Value domain (e.g. ACT_ECON) of each code column.
    """
    table = service_context.full_table_name(data_product.get_table("domaine_valeur"))
    codes: dict[str, list[str]] = defaultdict(list)
    for row in spark.table(table).select("domaine", "code").collect():
        codes[row["domaine"]].append(row["code"])
    checks = []
    for column, domain in columns.items():
        if not codes[domain]:
            logger.warning("No code in the %s domain of %s: %s is not checked", domain, table, column)
            continue
        checks.append(
            {
                "name": f"{column}_is_not_in_domain",
                "criticality": "warn",
                "check": {
                    "function": "is_in_list",
                    "arguments": {"column": column, "allowed": literal_values(sorted(codes[domain]))},
                },
            }
        )
    return checks
