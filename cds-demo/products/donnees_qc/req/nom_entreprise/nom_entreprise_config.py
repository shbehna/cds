"""Configuration of the nom_entreprise contract (source file Nom.csv)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cds import ContractConfig

from ..sources import read_source, select_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cds import RuntimeConfiguration

# Column of Nom.csv of each column of the contract.
COLUMNS = {
    "numero_entreprise": "NEQ",
    "nom": "NOM_ASSUJ",
    "nom_langue_etrangere": "NOM_ASSUJ_LANG_ETRNG",
    "code_statut_nom": "STAT_NOM",
    "code_type_nom": "TYP_NOM_ASSUJ",
    "date_inscription": "DAT_INIT_NOM_ASSUJ",
    "date_retrait": "DAT_FIN_NOM_ASSUJ",
}


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of Nom.csv; the service casts them to the contract types."""
    return select_columns(read_source(spark, runtime_configuration, "Nom.csv"), COLUMNS)


config = ContractConfig(name="nom_entreprise", transform=transform)
