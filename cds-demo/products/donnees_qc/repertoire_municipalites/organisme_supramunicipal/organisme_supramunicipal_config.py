"""Configuration of the organisme_supramunicipal contract (source file MRC_CM_Arg.csv)."""

from pyspark.sql import DataFrame, SparkSession

from cds import ContractConfig, RuntimeConfiguration

from ...sources import read_source
from ..sources import text


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of MRC_CM_Arg.csv; the service casts them to the contract types."""
    source = read_source(spark, runtime_configuration, "MRC_CM_Arg.csv")
    return source.select(
        text("mrccod").alias("code_geographique"),
        text("mrcnom").alias("nom"),
        text("mrcadr1").alias("adresse_physique_ligne_1"),
        text("mrcadr2").alias("adresse_physique_ligne_2"),
        text("mrcadr3").alias("adresse_postale_ligne_1"),
        text("mrcadr4").alias("adresse_postale_ligne_2"),
        text("mrccodp").alias("code_postal"),
        text("mrctel").alias("telephone"),
        text("mrcfax").alias("telecopieur"),
        text("mrccourriel").alias("courriel"),
        text("mrcweb").alias("site_web"),
        text("mrcdateconsti").alias("date_constitution"),
        text("mrcpopul").alias("population"),
        text("mrcregion").alias("region_administrative"),
        text("mrccodedesi").alias("code_designation"),
        text("mrcdescdesi").alias("designation"),
        text("msupf").alias("superficie"),
        text("msupft").alias("superficie_terrestre"),
        text("president").alias("president"),
        text("prefet").alias("prefet"),
        text("tres").alias("tresorier"),
        text("sectres").alias("secretaire_tresorier"),
        text("amenage").alias("responsable_amenagement"),
        text("urbanisme").alias("responsable_urbanisme"),
    )


config = ContractConfig(name="organisme_supramunicipal", transform=transform)
