"""Configuration of the etablissement contract (source file Etablissements.csv)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql.functions import col

from cds import ContractConfig

from ..domains import domain_checks
from ..sources import indicator, read_source

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cds import DataProduct, RuntimeConfiguration, ServiceContext


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of Etablissements.csv; the service casts them to the contract types."""
    source = read_source(spark, runtime_configuration, "Etablissements.csv")
    return source.select(
        col("NEQ").alias("numero_entreprise"),
        col("NO_SUF_ETAB").alias("numero_etablissement"),
        indicator("IND_ETAB_PRINC").alias("indicateur_etablissement_principal"),
        indicator("IND_SALON_BRONZ").alias("indicateur_salon_bronzage"),
        indicator("IND_VENTE_TABAC_DETL").alias("indicateur_vente_tabac"),
        indicator("IND_DISP").alias("indicateur_dispense_adresse"),
        col("LIGN1_ADR").alias("adresse_ligne_1"),
        col("LIGN2_ADR").alias("adresse_ligne_2"),
        col("LIGN3_ADR").alias("adresse_ligne_3"),
        col("LIGN4_ADR").alias("adresse_ligne_4"),
        col("COD_ACT_ECON").alias("code_premiere_activite"),
        col("DESC_ACT_ECON_ETAB").alias("description_premiere_activite"),
        col("NO_ACT_ECON_ETAB").alias("rang_premiere_activite"),
        col("COD_ACT_ECON2").alias("code_deuxieme_activite"),
        col("DESC_ACT_ECON_ETAB2").alias("description_deuxieme_activite"),
        col("NO_ACT_ECON_ETAB2").alias("rang_deuxieme_activite"),
        col("NOM_ETAB").alias("nom_etablissement"),
    )


def quality_checks(
    spark: SparkSession, service_context: ServiceContext, data_product: DataProduct
) -> list[dict[str, Any]]:
    """Checks the activity codes of the establishments against the value domains of the register."""
    return domain_checks(
        spark,
        service_context,
        data_product,
        {"code_premiere_activite": "ACT_ECON", "code_deuxieme_activite": "ACT_ECON"},
    )


config = ContractConfig(name="etablissement", transform=transform, quality_checks=quality_checks)
