"""Configuration of the municipalite contract (source file MUN.csv)."""

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from cds import ContractConfig, RuntimeConfiguration

from ...sources import read_source, uses_synthetic_data
from ..sources import oui_non, text

# The directory counts more than a thousand municipalities: fewer rows reveal a truncated
# file. The synthetic data set, which does not cover this data product yet, will have a
# few hundred rows, checked against a lower threshold.
MINIMUM_ROWS = 1_000
MINIMUM_SYNTHETIC_ROWS = 100


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Renames the columns of MUN.csv; the service casts them to the contract types.

    The councillors (con1 to con75) are in the conseiller_municipal table. The nd column is
    not used by the publisher.
    """
    source = read_source(spark, runtime_configuration, "MUN.csv")
    return source.select(
        text("mcode").alias("code_geographique"),
        text("munnom").alias("nom"),
        text("madr1").alias("adresse_physique_ligne_1"),
        text("madr2").alias("adresse_physique_ligne_2"),
        text("madr3").alias("adresse_postale_ligne_1"),
        text("madr4").alias("adresse_postale_ligne_2"),
        text("mcodpos").alias("code_postal"),
        text("mcourriel").alias("courriel"),
        text("mweb").alias("site_web"),
        text("mtel").alias("telephone"),
        text("mfax").alias("telecopieur"),
        text("mcodedesi").alias("code_designation"),
        text("mdes").alias("designation"),
        text("mgentile").alias("gentile"),
        text("regadm").alias("region_administrative"),
        text("divrec").alias("division_recensement"),
        text("mrc").alias("municipalite_regionale_comte"),
        text("admregionale").alias("administration_regionale"),
        oui_non("horsmrcautoch").alias("indicateur_autochtone_hors_municipalite_regionale_comte"),
        text("mdatcons").alias("date_constitution"),
        text("mdatregi").alias("date_changement_regime"),
        text("msuperf").alias("superficie"),
        text("msupft").alias("superficie_terrestre"),
        text("mpopul").alias("population"),
        text("datelec").alias("date_prochaine_election"),
        text("electype").alias("code_type_election"),
        text("delectype").alias("type_election"),
        text("elecmode").alias("code_mode_election"),
        text("delecmode").alias("mode_election"),
        text("divter").alias("division_territoriale"),
        text("mcm").alias("communaute_metropolitaine"),
        text("mcirc").alias("circonscriptions_electorales"),
        text("maire").alias("maire"),
        text("dirgen").alias("directeur_general"),
        text("dirsecpub").alias("directeur_securite_publique"),
        text("tres").alias("tresorier"),
        text("gref").alias("greffier"),
        text("sectres").alias("secretaire_tresorier"),
        text("polic").alias("directeur_police"),
        text("incen").alias("directeur_incendie"),
        text("loisir").alias("directeur_loisirs"),
        text("trvpub").alias("directeur_travaux_publics"),
        text("mesurg").alias("coordonnateur_mesures_urgence"),
        text("urban").alias("responsable_urbanisme"),
        text("communic").alias("directeur_communications"),
        text("permis").alias("responsable_permis"),
        text("batim").alias("inspecteur_municipal"),
    )


def row_count_check(runtime_configuration: RuntimeConfiguration) -> dict[str, Any]:
    """Warns when the table has fewer rows than expected, from the directory or the synthetic data set."""
    minimum = MINIMUM_SYNTHETIC_ROWS if uses_synthetic_data(runtime_configuration) else MINIMUM_ROWS
    return {
        "name": "row_count_minimum",
        "criticality": "warn",
        "check": {
            "function": "is_aggr_not_less_than",
            "arguments": {"column": "*", "aggr_type": "count", "limit": minimum},
        },
    }


def quality_checks(runtime_configuration: RuntimeConfiguration) -> list[dict[str, Any]]:
    """Checks the number of municipalities."""
    return [row_count_check(runtime_configuration)]


config = ContractConfig(name="municipalite", transform=transform, quality_checks=quality_checks)
