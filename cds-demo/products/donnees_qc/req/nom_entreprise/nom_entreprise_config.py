"""Configuration of the nom_entreprise contract (source file Nom.csv)."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from cds import ContractConfig, RuntimeConfiguration

from ..sources import read_source


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of Nom.csv; the service casts them to the contract types."""
    source = read_source(spark, runtime_configuration, "Nom.csv")
    return source.select(
        col("NEQ").alias("numero_entreprise"),
        col("NOM_ASSUJ").alias("nom"),
        col("NOM_ASSUJ_LANG_ETRNG").alias("nom_langue_etrangere"),
        col("STAT_NOM").alias("code_statut_nom"),
        col("TYP_NOM_ASSUJ").alias("code_type_nom"),
        col("DAT_INIT_NOM_ASSUJ").alias("date_inscription"),
        col("DAT_FIN_NOM_ASSUJ").alias("date_retrait"),
    )


config = ContractConfig(name="nom_entreprise", transform=transform)
