"""Configuration of the continuation_transformation contract (source file ContinuationsTransformations.csv)."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from cds import ContractConfig, RuntimeConfiguration

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


config = ContractConfig(name="continuation_transformation", transform=transform)
