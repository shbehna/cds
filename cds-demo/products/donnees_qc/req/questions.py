"""Questions of the Genie space of req: the instructions, sample questions and examples it adds to the contracts.

The space is in French, the language of the register.
"""

from cds import DataProduct, GenieExample, ServiceContext
from cds.genie import error_rate_sql
from cds.sql import quote_name, string_literal

# The value domain of the economic activities, in domaine_valeur.
ACTIVITY_DOMAIN = "ACT_ECON"

INSTRUCTIONS = (
    "Les codes dont la description de la colonne ne liste pas les valeurs sont décrits dans la table "
    "domaine_valeur. La description de chaque colonne de code nomme son domaine de valeur, par exemple "
    "« domaine de valeur REGIM_JURI » : joignez domaine_valeur sur upper(trim(domaine)) = ce domaine et "
    "trim(code) = la colonne, puis montrez la description du code plutôt que le code.",
    "Répondez en français.",
)

SAMPLE_QUESTIONS = (
    "Combien d'entreprises sont immatriculées, par forme juridique ?",
    "Combien d'établissements compte en moyenne une entreprise ?",
    "Combien de continuations et de transformations ont pris effet chaque année ?",
)


def top_activities_sql(service_context: ServiceContext, data_product: DataProduct) -> str:
    """The most frequent economic activities of the enterprises, described by their value domain."""
    enterprise = quote_name(service_context.full_table_name(data_product.get_table("entreprise")))
    domains = quote_name(service_context.full_table_name(data_product.get_table("domaine_valeur")))
    return (
        "SELECT d.`description` AS activite, count(*) AS entreprises\n"
        f"FROM {enterprise} AS e\n"
        f"JOIN {domains} AS d\n"
        f"  ON upper(trim(d.`domaine`)) = {string_literal(ACTIVITY_DOMAIN)} "
        "AND trim(d.`code`) = e.`code_premiere_activite`\n"
        "GROUP BY d.`description`\n"
        "ORDER BY entreprises DESC\n"
        "LIMIT 10"
    )


EXAMPLES = (
    GenieExample("Quelles sont les 10 activités économiques les plus fréquentes des entreprises ?", top_activities_sql),
    GenieExample("Quelle part des lignes de chaque table a une erreur de qualité ?", error_rate_sql),
)
