"""Configuration of the ressource contract (source file ress.csv)."""

from pyspark.sql import DataFrame, SparkSession

from cds import ContractConfig, RuntimeConfiguration

from ...sources import read_source
from ..sources import oui_non, text


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of ress.csv; the service casts them to the contract types."""
    source = read_source(spark, runtime_configuration, "ress.csv")
    return source.select(
        text("rescode").alias("code_organisation"),
        text("resgroupe").alias("groupe_titre"),
        text("rescodetitre").alias("code_titre"),
        text("restitre").alias("titre"),
        text("resentreefonction").alias("date_entree_fonction"),
        text("resnom").alias("nom"),
        text("resprenom").alias("prenom"),
        oui_non("resestvacant").alias("indicateur_poste_vacant"),
    )


config = ContractConfig(name="ressource", transform=transform)
