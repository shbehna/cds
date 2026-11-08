"""Configuration of the entreprise contract (source file Entreprise.csv)."""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from cds import ContractConfig, DataProduct, RuntimeConfiguration, ServiceContext

from ..domains import domain_checks
from ..sources import indicator, read_source


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of Entreprise.csv; the service casts them to the contract types."""
    source = read_source(spark, runtime_configuration, "Entreprise.csv")
    return source.select(
        col("NEQ").alias("numero_entreprise"),
        indicator("IND_FAIL").alias("indicateur_faillite"),
        col("DAT_IMMAT").alias("date_immatriculation"),
        col("COD_REGIM_JURI").alias("code_regime_juridique"),
        col("COD_INTVAL_EMPLO_QUE").alias("code_intervalle_employes"),
        col("DAT_CESS_PREVU").alias("date_cessation_prevue"),
        col("COD_STAT_IMMAT").alias("code_statut_immatriculation"),
        col("COD_FORME_JURI").alias("code_forme_juridique"),
        col("DAT_STAT_IMMAT").alias("date_statut_immatriculation"),
        col("COD_REGIM_JURI_CONSTI").alias("code_regime_juridique_constitution"),
        col("DAT_DEPO_DECLR").alias("date_depot_declaration"),
        col("AN_DECL").alias("annee_declaration"),
        col("AN_PROD").alias("annee_production"),
        col("DAT_LIMIT_PROD").alias("date_limite_production"),
        col("AN_PROD_PRE").alias("annee_production_precedente"),
        col("DAT_LIMIT_PROD_PRE").alias("date_limite_production_precedente"),
        col("DAT_MAJ_INDEX_NOM").alias("date_mise_a_jour_index_noms"),
        col("COD_ACT_ECON_CAE").alias("code_premiere_activite"),
        col("NO_ACT_ECON_ASSUJ").alias("rang_premiere_activite"),
        col("DESC_ACT_ECON_ASSUJ").alias("description_premiere_activite"),
        col("COD_ACT_ECON_CAE2").alias("code_deuxieme_activite"),
        col("NO_ACT_ECON_ASSUJ2").alias("rang_deuxieme_activite"),
        col("DESC_ACT_ECON_ASSUJ2").alias("description_deuxieme_activite"),
        col("NOM_LOCLT_CONSTI").alias("localite_constitution"),
        col("DAT_CONSTI").alias("date_constitution"),
        indicator("IND_CONVEN_UNMN_ACTNR").alias("indicateur_convention_unanime"),
        indicator("IND_RET_TOUT_POUVR").alias("indicateur_retrait_pouvoirs"),
        indicator("IND_LIMIT_RESP").alias("indicateur_responsabilite_limitee"),
        col("DAT_DEB_RESP").alias("date_debut_responsabilite_limitee"),
        col("DAT_FIN_RESP").alias("date_fin_responsabilite_limitee"),
        col("OBJET_SOC").alias("objet_societe"),
        col("NO_MTR_VOLONT").alias("numero_autorite_publique"),
        indicator("ADR_DOMCL_ADR_DISP").alias("indicateur_dispense_adresse"),
        col("ADR_DOMCL_LIGN1_ADR").alias("adresse_ligne_1"),
        col("ADR_DOMCL_LIGN2_ADR").alias("adresse_ligne_2"),
        col("ADR_DOMCL_LIGN3_ADR").alias("adresse_ligne_3"),
        col("ADR_DOMCL_LIGN4_ADR").alias("adresse_ligne_4"),
    )


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
