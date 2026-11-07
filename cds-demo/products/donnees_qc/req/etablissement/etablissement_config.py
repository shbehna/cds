"""Configuration of the etablissement contract (source file Etablissements.csv)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cds import ContractConfig

from ..domains import domain_checks
from ..sources import indicator, read_source, select_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cds import DataProduct, RuntimeConfiguration, ServiceContext

# Expression of each column of the contract: a column of Etablissements.csv, or SQL.
COLUMNS = {
    "numero_entreprise": "NEQ",
    "numero_etablissement": "NO_SUF_ETAB",
    "indicateur_etablissement_principal": indicator("IND_ETAB_PRINC"),
    "indicateur_salon_bronzage": indicator("IND_SALON_BRONZ"),
    "indicateur_vente_tabac": indicator("IND_VENTE_TABAC_DETL"),
    "indicateur_dispense_adresse": indicator("IND_DISP"),
    "adresse_ligne_1": "LIGN1_ADR",
    "adresse_ligne_2": "LIGN2_ADR",
    "adresse_ligne_3": "LIGN3_ADR",
    "adresse_ligne_4": "LIGN4_ADR",
    "code_premiere_activite": "COD_ACT_ECON",
    "description_premiere_activite": "DESC_ACT_ECON_ETAB",
    "rang_premiere_activite": "NO_ACT_ECON_ETAB",
    "code_deuxieme_activite": "COD_ACT_ECON2",
    "description_deuxieme_activite": "DESC_ACT_ECON_ETAB2",
    "rang_deuxieme_activite": "NO_ACT_ECON_ETAB2",
    "nom_etablissement": "NOM_ETAB",
}


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of Etablissements.csv; the service casts them to the contract types."""
    return select_columns(read_source(spark, runtime_configuration, "Etablissements.csv"), COLUMNS)


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
