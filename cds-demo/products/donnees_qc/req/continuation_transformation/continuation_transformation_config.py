"""Configuration of the continuation_transformation contract (source file ContinuationsTransformations.csv)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cds import ContractConfig

from ..domains import domain_checks
from ..sources import read_source, select_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cds import DataProduct, RuntimeConfiguration, ServiceContext

# Column of ContinuationsTransformations.csv of each column of the contract.
COLUMNS = {
    "numero_entreprise": "NEQ",
    "code_type_changement": "COD_TYP_CHANG",
    "code_regime_juridique": "COD_REGIM_JURI",
    "autre_regime_juridique": "AUTR_REGIM_JURI",
    "localite": "NOM_LOCLT",
    "date_effet": "DAT_EFCTVT",
}


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of ContinuationsTransformations.csv; the service casts them to the contract types."""
    return select_columns(read_source(spark, runtime_configuration, "ContinuationsTransformations.csv"), COLUMNS)


def quality_checks(
    spark: SparkSession, service_context: ServiceContext, data_product: DataProduct
) -> list[dict[str, Any]]:
    """Checks the governing law codes against the value domains of the register."""
    return domain_checks(spark, service_context, data_product, {"code_regime_juridique": "REGIM_JURI"})


config = ContractConfig(name="continuation_transformation", transform=transform, quality_checks=quality_checks)
