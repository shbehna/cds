"""Configuration of the continuation_transformation contract (source file ContinuationsTransformations.csv)."""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from cds import ContractConfig, DataProduct, RuntimeConfiguration, ServiceContext

from ..domains import domain_checks
from ..sources import read_source


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of ContinuationsTransformations.csv; the service casts them to the contract types."""
    source = read_source(spark, runtime_configuration, "ContinuationsTransformations.csv")
    return source.select(
        col("NEQ").alias("numero_entreprise"),
        col("COD_TYP_CHANG").alias("code_type_changement"),
        col("COD_REGIM_JURI").alias("code_regime_juridique"),
        col("AUTR_REGIM_JURI").alias("autre_regime_juridique"),
        col("NOM_LOCLT").alias("localite"),
        col("DAT_EFCTVT").alias("date_effet"),
    )


def quality_checks(
    spark: SparkSession, service_context: ServiceContext, data_product: DataProduct
) -> list[dict[str, Any]]:
    """Checks the governing law codes against the value domains of the register."""
    return domain_checks(spark, service_context, data_product, {"code_regime_juridique": "REGIM_JURI"})


config = ContractConfig(name="continuation_transformation", transform=transform, quality_checks=quality_checks)
