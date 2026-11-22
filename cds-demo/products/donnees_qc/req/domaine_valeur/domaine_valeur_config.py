"""Configuration of the domaine_valeur contract (source file DomaineValeur.csv)."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from cds import ContractConfig, RuntimeConfiguration

from ...sources import read_source


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of DomaineValeur.csv; the service casts them to the contract types."""
    source = read_source(spark, runtime_configuration, "DomaineValeur.csv")
    return source.select(
        col("TYP_DOM_VAL").alias("domaine"),
        col("COD_DOM_VAL").alias("code"),
        col("VAL_DOM_FRAN").alias("description"),
    )


config = ContractConfig(name="domaine_valeur", transform=transform)
