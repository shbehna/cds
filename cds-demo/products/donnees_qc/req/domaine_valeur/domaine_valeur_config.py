"""Configuration of the domaine_valeur contract (source file DomaineValeur.csv)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cds import ContractConfig

from ..sources import read_source, select_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cds import RuntimeConfiguration

# Column of DomaineValeur.csv of each column of the contract.
COLUMNS = {
    "domaine": "TYP_DOM_VAL",
    "code": "COD_DOM_VAL",
    "description": "VAL_DOM_FRAN",
}


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of DomaineValeur.csv; the service casts them to the contract types."""
    return select_columns(read_source(spark, runtime_configuration, "DomaineValeur.csv"), COLUMNS)


config = ContractConfig(name="domaine_valeur", transform=transform)
