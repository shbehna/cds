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
    "trim(code) = la colonne, puis montrez la description du code plutôt que le code. Quelques codes y sont "
    "écrits plusieurs fois, avec un domaine en minuscules ou des espaces : ne gardez qu'une ligne par domaine "
    "et code normalisés (QUALIFY row_number() OVER (PARTITION BY upper(trim(domaine)), trim(code) ORDER BY "
    "domaine, code) = 1), pour ne pas compter deux fois les lignes jointes.",
    "Répondez en français.",
)

SAMPLE_QUESTIONS = (
    "Combien d'entreprises sont immatriculées, par forme juridique ?",
    "Combien d'établissements compte en moyenne une entreprise ?",
    "Combien de continuations et de transformations ont pris effet chaque année ?",
)


def top_activities_sql(service_context: ServiceContext, data_product: DataProduct) -> str:
    """The most frequent economic activities of the enterprises, described by their value domain.

    The key of domaine_valeur is its raw domain and code: a code written twice, with its
    domain in lowercase or with spaces, which a quality check reports, is kept once, so
    that the enterprises are not counted twice.
    """
    enterprise = quote_name(service_context.full_table_name(data_product.get_table("entreprise")))
    domains = quote_name(service_context.full_table_name(data_product.get_table("domaine_valeur")))
    return (
        "WITH activites AS (\n"
        "  SELECT trim(`code`) AS code, `description`\n"
        f"  FROM {domains}\n"
        f"  WHERE upper(trim(`domaine`)) = {string_literal(ACTIVITY_DOMAIN)}\n"
        "  QUALIFY row_number() OVER (PARTITION BY trim(`code`) ORDER BY `domaine`, `code`) = 1\n"
        ")\n"
        "SELECT a.`description` AS activite, count(*) AS entreprises\n"
        f"FROM {enterprise} AS e\n"
        "JOIN activites AS a ON a.`code` = e.`code_premiere_activite`\n"
        "GROUP BY a.`description`\n"
        "ORDER BY entreprises DESC\n"
        "LIMIT 10"
    )


EXAMPLES = (
    GenieExample("Quelles sont les 10 activités économiques les plus fréquentes des entreprises ?", top_activities_sql),
    GenieExample("Quelle part des lignes de chaque table a une erreur de qualité ?", error_rate_sql),
)
