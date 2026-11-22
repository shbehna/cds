"""Questions of the Genie space of repertoire_municipalites: what it adds to the contracts.

The instructions, sample questions and examples of the space, in French, the language of
the directory.
"""

from cds import DataProduct, GenieExample, ServiceContext
from cds.genie import error_rate_sql
from cds.sql import quote_name

INSTRUCTIONS = (
    "Les conseils sont dans conseiller_municipal et conseiller_arrondissement, une ligne par siège. Un siège "
    "vacant a indicateur_poste_vacant = true et pas de nom : excluez-le pour compter les conseillers, "
    "comptez-le pour compter les sièges.",
    "La table ressource n'a pas de clé étrangère : code_organisation est le code géographique d'une "
    "municipalité (municipalite.code_geographique), d'un organisme supramunicipal "
    "(organisme_supramunicipal.code_geographique), d'un arrondissement (arrondissement.code_geographique) ou "
    "d'une agglomération (code commençant par AG, sans table). Joignez la table de l'organisation voulue sur "
    "code_geographique = code_organisation.",
    "Les colonnes region_administrative, division_recensement, municipalite_regionale_comte et "
    "communaute_metropolitaine contiennent un nom suivi de son code entre parenthèses, par exemple « Estrie "
    "(05) » : groupez sur la valeur entière.",
    "Le répertoire note les valeurs inconnues : le numéro de téléphone 0000000000, la date d'élection "
    "0001-01-01 et les dates d'entrée en fonction de 1900. Excluez-les des calculs sur ces colonnes.",
    "Répondez en français.",
)

SAMPLE_QUESTIONS = (
    "Combien de municipalités compte chaque région administrative ?",
    "Quelles sont les 10 municipalités les plus peuplées ?",
    "Combien de sièges des conseils municipaux sont vacants ?",
)


def population_by_region_sql(service_context: ServiceContext, data_product: DataProduct) -> str:
    """The population and the number of municipalities of each administrative region."""
    municipalities = quote_name(service_context.full_table_name(data_product.get_table("municipalite")))
    return (
        "SELECT `region_administrative`, count(*) AS municipalites, sum(`population`) AS population\n"
        f"FROM {municipalities}\n"
        "GROUP BY `region_administrative`\n"
        "ORDER BY population DESC"
    )


EXAMPLES = (
    GenieExample("Quelle est la population de chaque région administrative ?", population_by_region_sql),
    GenieExample("Quelle part des lignes de chaque table a une erreur de qualité ?", error_rate_sql),
)
