"""Configuration of the arrondissement contract (source file ARR.csv)."""

from pyspark.sql import DataFrame, SparkSession

from cds import ContractConfig, RuntimeConfiguration

from ...sources import read_source
from ..sources import oui_non, text


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of ARR.csv; the service casts them to the contract types.

    The councillors (con01 to con75) are in the conseiller_arrondissement table.
    """
    source = read_source(spark, runtime_configuration, "ARR.csv")
    return source.select(
        text("arrcod").alias("code_geographique"),
        text("arrnom").alias("nom"),
        text("arrville").alias("nom_municipalite"),
        text("arrcode").alias("code_geographique_municipalite"),
        text("arradr1").alias("adresse_physique_ligne_1"),
        text("arradr2").alias("adresse_physique_ligne_2"),
        text("arradr3").alias("adresse_postale_ligne_1"),
        text("arradr4").alias("adresse_postale_ligne_2"),
        text("arrcodp").alias("code_postal"),
        text("arrtel").alias("telephone"),
        text("arrfax").alias("telecopieur"),
        text("arrcourriel").alias("courriel"),
        text("arrregion").alias("region_administrative"),
        text("arrmrc").alias("municipalite_regionale_comte"),
        oui_non("arrhorsmrcautoch").alias("indicateur_autochtone_hors_municipalite_regionale_comte"),
        text("arrcm").alias("communaute_metropolitaine"),
        text("arrgentile").alias("gentile"),
        text("arradrinternet").alias("site_web"),
        text("arrdateconsti").alias("date_constitution"),
        text("arrdateproelec").alias("date_prochaine_election"),
        text("president").alias("president"),
        text("directeur").alias("directeur"),
    )


config = ContractConfig(name="arrondissement", transform=transform)
