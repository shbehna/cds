"""Configuration of the fusion_scission contract (source file FusionScissions.csv)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cds import ContractConfig

from ..sources import indicator, read_source, select_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cds import RuntimeConfiguration

# Expression of each column of the contract: a column of FusionScissions.csv, or SQL.
COLUMNS = {
    "numero_entreprise": "NEQ",
    "numero_entreprise_liee": "NEQ_ASSUJ_REL",
    "nom_entreprise_liee": "DENOMN_SOC",
    "code_relation": "COD_RELA_ASSUJ",
    "date_effet": "DAT_EFCTVT",
    "indicateur_dispense_adresse": indicator("IND_DISP"),
    "adresse_ligne_1": "LIGN1_ADR",
    "adresse_ligne_2": "LIGN2_ADR",
    "adresse_ligne_3": "LIGN3_ADR",
    "adresse_ligne_4": "LIGN4_ADR",
}


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of FusionScissions.csv; the service casts them to the contract types."""
    return select_columns(read_source(spark, runtime_configuration, "FusionScissions.csv"), COLUMNS)


config = ContractConfig(name="fusion_scission", transform=transform)
