"""Configuration of the entreprise contract (source file Entreprise.csv)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cds import ContractConfig

from ..domains import domain_checks
from ..sources import indicator, read_source, select_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cds import DataProduct, RuntimeConfiguration, ServiceContext

# Expression of each column of the contract: a column of Entreprise.csv, or SQL.
COLUMNS = {
    "numero_entreprise": "NEQ",
    "indicateur_faillite": indicator("IND_FAIL"),
    "date_immatriculation": "DAT_IMMAT",
    "code_regime_juridique": "COD_REGIM_JURI",
    "code_intervalle_employes": "COD_INTVAL_EMPLO_QUE",
    "date_cessation_prevue": "DAT_CESS_PREVU",
    "code_statut_immatriculation": "COD_STAT_IMMAT",
    "code_forme_juridique": "COD_FORME_JURI",
    "date_statut_immatriculation": "DAT_STAT_IMMAT",
    "code_regime_juridique_constitution": "COD_REGIM_JURI_CONSTI",
    "date_depot_declaration": "DAT_DEPO_DECLR",
    "annee_declaration": "AN_DECL",
    "annee_production": "AN_PROD",
    "date_limite_production": "DAT_LIMIT_PROD",
    "annee_production_precedente": "AN_PROD_PRE",
    "date_limite_production_precedente": "DAT_LIMIT_PROD_PRE",
    "date_mise_a_jour_index_noms": "DAT_MAJ_INDEX_NOM",
    "code_premiere_activite": "COD_ACT_ECON_CAE",
    "rang_premiere_activite": "NO_ACT_ECON_ASSUJ",
    "description_premiere_activite": "DESC_ACT_ECON_ASSUJ",
    "code_deuxieme_activite": "COD_ACT_ECON_CAE2",
    "rang_deuxieme_activite": "NO_ACT_ECON_ASSUJ2",
    "description_deuxieme_activite": "DESC_ACT_ECON_ASSUJ2",
    "localite_constitution": "NOM_LOCLT_CONSTI",
    "date_constitution": "DAT_CONSTI",
    "indicateur_convention_unanime": indicator("IND_CONVEN_UNMN_ACTNR"),
    "indicateur_retrait_pouvoirs": indicator("IND_RET_TOUT_POUVR"),
    "indicateur_responsabilite_limitee": indicator("IND_LIMIT_RESP"),
    "date_debut_responsabilite_limitee": "DAT_DEB_RESP",
    "date_fin_responsabilite_limitee": "DAT_FIN_RESP",
    "objet_societe": "OBJET_SOC",
    "numero_autorite_publique": "NO_MTR_VOLONT",
    "indicateur_dispense_adresse": indicator("ADR_DOMCL_ADR_DISP"),
    "adresse_ligne_1": "ADR_DOMCL_LIGN1_ADR",
    "adresse_ligne_2": "ADR_DOMCL_LIGN2_ADR",
    "adresse_ligne_3": "ADR_DOMCL_LIGN3_ADR",
    "adresse_ligne_4": "ADR_DOMCL_LIGN4_ADR",
}


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of Entreprise.csv; the service casts them to the contract types."""
    return select_columns(read_source(spark, runtime_configuration, "Entreprise.csv"), COLUMNS)


def quality_checks(
    spark: SparkSession, service_context: ServiceContext, data_product: DataProduct
) -> list[dict[str, Any]]:
    """Checks the codes of the enterprises against the value domains of the register."""
    return domain_checks(
        spark,
        service_context,
        data_product,
        {
            "code_regime_juridique": "REGIM_JURI",
            "code_regime_juridique_constitution": "REGIM_JURI",
            "code_premiere_activite": "ACT_ECON",
            "code_deuxieme_activite": "ACT_ECON",
        },
    )


config = ContractConfig(name="entreprise", transform=transform, quality_checks=quality_checks)
