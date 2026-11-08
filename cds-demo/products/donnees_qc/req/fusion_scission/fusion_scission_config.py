"""Configuration of the fusion_scission contract (source file FusionScissions.csv)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from cds import ContractConfig, RuntimeConfiguration

from ..sources import indicator, read_source


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of FusionScissions.csv; the service casts them to the contract types."""
    source = read_source(spark, runtime_configuration, "FusionScissions.csv")
    return source.select(
        col("NEQ").alias("numero_entreprise"),
        col("NEQ_ASSUJ_REL").alias("numero_entreprise_liee"),
        col("DENOMN_SOC").alias("nom_entreprise_liee"),
        col("COD_RELA_ASSUJ").alias("code_relation"),
        col("DAT_EFCTVT").alias("date_effet"),
        indicator("IND_DISP").alias("indicateur_dispense_adresse"),
        col("LIGN1_ADR").alias("adresse_ligne_1"),
        col("LIGN2_ADR").alias("adresse_ligne_2"),
        col("LIGN3_ADR").alias("adresse_ligne_3"),
        col("LIGN4_ADR").alias("adresse_ligne_4"),
    )


config = ContractConfig(name="fusion_scission", transform=transform)
