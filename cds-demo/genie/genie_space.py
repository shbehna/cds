"""Generates the Genie space of the REQ data product, from its data contracts.

A Genie space answers questions in natural language by writing SQL on the tables of the
data product. Genie reads the comments of the tables and columns in Unity Catalog, which
the metadata service sets from the contracts; this space adds what the comments do not
say, all of it taken from the contracts and the data product document:

- the tables of the data product (``data_sources``);
- the joins of the tables, from the ``foreignKey`` relationships of the contracts
  (``join_specs``);
- the values of the codes and their labels, from the ``enum`` of the properties, in the
  description of their columns (``column_configs``);
- the instructions: the purpose, usage and limitations of the data product (ODPS), the
  keys of the tables, the deprecated contracts, the quality results of each row and the
  error rate of the service levels, the classified columns, and the frequency of the
  loads (``text_instructions``);
- example questions, with their SQL when it is generated from the contracts
  (``sample_questions`` and ``example_question_sqls``).

The space is written in French, the language of the register. Its tables are named in the
catalog and schema of the naming conventions, with the placeholder ``${environment}`` for
the environment, replaced by the deployment. The ``serialized_space`` of the file is the
configuration of the Genie API (version 2), whose lists must be sorted: the deployment
serializes it as a JSON string.

The generated file is committed; regenerate it, from the root of the repository, after
changing the contracts or their configurations:

    uv run python cds-demo/genie/genie_space.py
"""

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cds import DataContext, DataProduct, QualitySettings, ServiceContext, Table
from cds.config import resolved
from cds.errors import ConfigurationError
from cds.odcs import Relationship, SchemaProperty
from cds.sla import percentage, table_sla
from cds.sql import quote_identifier, quote_name, string_literal

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
SPACE = Path(__file__).with_name("req_genie_space.json")
DATA_PRODUCT = "donnees_qc.req"
# Version of the serialized space of the Genie API.
VERSION = 2
# Placeholder of the environment in the names of the tables, replaced by the deployment.
ENVIRONMENT = "${environment}"
# Environment id naming the tables, then replaced by the placeholder.
_SENTINEL = "cds_genie_environment"
# Classification of the columns readable by everyone.
PUBLIC = "public"
# Statuses of the contracts whose tables should not be used.
DEPRECATED = ("deprecated", "retired")
# The relationship of a join, as the Genie API annotates its SQL.
MANY_TO_ONE = "MANY_TO_ONE"
ONE_TO_ONE = "ONE_TO_ONE"

# Category of each kind of item, the first byte of its id: the ids of the instructions,
# examples and joins must be unique together, and those of the questions too.
_SAMPLE_QUESTION = 1
_TEXT_INSTRUCTION = 2
_EXAMPLE_SQL = 3
_JOIN = 4

# The value domain of the economic activities, in domaine_valeur.
ACTIVITY_DOMAIN = "ACT_ECON"
TOP_ACTIVITIES_QUESTION = "Quelles sont les 10 activités économiques les plus fréquentes des entreprises ?"
ERROR_RATE_QUESTION = "Quelle part des lignes de chaque table a une erreur de qualité ?"
# Questions shown to the users of the space, besides those of the examples.
SAMPLE_QUESTIONS = (
    "Combien d'entreprises sont immatriculées, par forme juridique ?",
    "Combien d'établissements compte en moyenne une entreprise ?",
    "Combien de continuations et de transformations ont pris effet chaque année ?",
    TOP_ACTIVITIES_QUESTION,
    ERROR_RATE_QUESTION,
)


def item_id(category: int, index: int) -> str:
    """Id of an item of the space: 32 lowercase hexadecimal digits, sorted by category, then index."""
    return f"{category:02x}{index:030x}"


def lines(text: str) -> list[str]:
    """A text as the Genie API stores it: a list of lines, each ending with its line break."""
    split = text.split("\n")
    return [line + "\n" for line in split[:-1]] + split[-1:]


def _context(data_context: DataContext) -> ServiceContext:
    return ServiceContext(spark=None, environment=_SENTINEL, data_context=data_context)


def _name(full_name: str) -> str:
    """A name of the naming conventions, with the placeholder of the environment."""
    return full_name.replace(_SENTINEL, ENVIRONMENT)


def _data_product(data_context: DataContext) -> DataProduct:
    return data_context.get_data_product(DATA_PRODUCT)


def _tables(data_context: DataContext) -> list[Table]:
    return sorted(_data_product(data_context).tables, key=lambda table: table.name)


def _column(table: Table, name: str) -> str:
    """The quoted name of a column of a table.

    Raises:
        ConfigurationError: If the contract of the table has no such column.
    """
    if table.get_column(name) is None:
        raise ConfigurationError(f"{table.qualified_name}: the Genie space uses the column {name!r}, which is unknown.")
    return quote_identifier(name)


def column_description(prop: SchemaProperty) -> str | None:
    """The description of a column, with the values of its ``enum`` and their labels."""
    values = ", ".join(f"{value.value} ({value.label})" if value.label else str(value.value) for value in prop.enum)
    parts = [prop.description, f"Valeurs : {values}." if values else None]
    return " ".join(part for part in parts if part) or None


def table_source(context: ServiceContext, table: Table) -> dict[str, Any]:
    """A table of the space, with the descriptions of its coded columns.

    The descriptions of the other columns are their comments in Unity Catalog.
    """
    source: dict[str, Any] = {"identifier": _name(context.full_table_name(table))}
    configs = [
        {"column_name": prop.column, "description": lines(description)}
        for prop in sorted(table.columns, key=lambda prop: prop.column)
        if prop.enum and (description := column_description(prop))
    ]
    if configs:
        source["column_configs"] = configs
    return source


def _relationships(table: Table) -> list[tuple[list[str], Relationship]]:
    """The relationships of a table with their source columns, in the order of the contract."""
    found = [([prop.column], rel) for prop in table.columns for rel in prop.relationships]
    found += [(list(rel.source), rel) for rel in table.schema_object.relationships]
    return found


def _alias(target: Table, sources: Sequence[str], columns: Sequence[str], origin: Table) -> str:
    """Alias of the referenced table of a join.

    The table keeps its name when the join uses its columns under the same names, as
    ``entreprise`` for ``numero_entreprise``; otherwise its name gets the suffix of the
    source columns: ``entreprise_liee`` for ``numero_entreprise_liee``.
    """
    if list(sources) == list(columns) and target is not origin:
        return target.name
    suffixes = [
        source.removeprefix(f"{column}_") if source.startswith(f"{column}_") else source
        for source, column in zip(sources, columns, strict=True)
    ]
    return f"{target.name}_{'_'.join(suffixes)}"


def join_specs(data_context: DataContext) -> list[dict[str, Any]]:
    """The joins of the tables, from the ``foreignKey`` relationships of their contracts.

    A join is ``ONE_TO_ONE`` when its columns are the primary key of the table declaring
    it, ``MANY_TO_ONE`` otherwise.

    Raises:
        ConfigurationError: If a relationship references several tables, or two joins of a
            table get the same alias.
    """
    context = _context(data_context)
    joins = []
    for table in _tables(data_context):
        aliases: set[str] = set()
        for sources, relationship in _relationships(table):
            if relationship.type != "foreignKey":
                continue
            targets = [data_context.resolve_reference(ref, table) for ref in relationship.target]
            if len({target.qualified_name for target, _ in targets}) != 1:
                raise ConfigurationError(f"{table.qualified_name}: a relationship must reference a single table.")
            target = targets[0][0]
            columns = [column for _, column in targets]
            alias = _alias(target, sources, columns, table)
            if alias in aliases or alias == table.name:
                raise ConfigurationError(f"{table.qualified_name}: two joins use the alias {alias!r}.")
            aliases.add(alias)
            condition = " AND ".join(
                f"{quote_identifier(table.name)}.{quote_identifier(source)} = "
                f"{quote_identifier(alias)}.{quote_identifier(column)}"
                for source, column in zip(sources, columns, strict=True)
            )
            cardinality = ONE_TO_ONE if sorted(sources) == sorted(table.primary_key) else MANY_TO_ONE
            joins.append(
                {
                    "left": {"identifier": _name(context.full_table_name(table)), "alias": table.name},
                    "right": {"identifier": _name(context.full_table_name(target)), "alias": alias},
                    "sql": [condition, f"--rt=FROM_RELATIONSHIP_TYPE_{cardinality}--"],
                }
            )
    return [{"id": item_id(_JOIN, index), **join} for index, join in enumerate(joins)]


def _keys(tables: Sequence[Table]) -> str:
    keys = [
        f"{table.name} ({', '.join(table.primary_key)})"
        if table.primary_key
        else f"{table.name} n'a pas de clé : ses lignes ne sont pas uniques"
        for table in tables
    ]
    return f"Clés des tables : {' ; '.join(keys)}."


def _quality(tables: Sequence[Table]) -> list[str]:
    checked = [table for table in tables if table.settings(QualitySettings).enabled]
    if not checked:
        return []
    columns = {
        (resolved(settings.errors_column), resolved(settings.warnings_column))
        for settings in (table.settings(QualitySettings) for table in checked)
    }
    if len(columns) != 1:
        raise ConfigurationError(f"{DATA_PRODUCT}: the quality result columns must be the same for all the tables.")
    [(errors, warnings)] = columns
    found = [table_sla(table, "errorRate") for table in checked]
    rates = {percentage(sla[0]) for sla in found if sla is not None}
    text = [
        f"Les contrôles de qualité écrivent leurs résultats dans chaque ligne : {errors} pour les erreurs, "
        f"{warnings} pour les avertissements. Ce sont des tableaux de résultats, avec le nom du contrôle (name), "
        f"son message (message) et ses colonnes (columns). Une ligne est en erreur si size({errors}) > 0.",
    ]
    if len(rates) == 1 and len(found) == len(checked):
        text.append(f"Le niveau de service tolère au plus {rates.pop():g} % de lignes en erreur par table.")
    return text


def _classified(tables: Sequence[Table]) -> str | None:
    classified = [
        f"{table.name}.{prop.column} ({prop.classification})"
        for table in tables
        for prop in table.columns
        if prop.classification not in (None, PUBLIC)
    ]
    if not classified:
        return None
    return (
        "Selon votre accès, les colonnes classifiées peuvent être masquées (****) : ne les utilisez ni pour "
        f"filtrer ni pour regrouper. Colonnes classifiées : {', '.join(classified)}."
    )


def _frequency(tables: Sequence[Table]) -> str | None:
    found = (table_sla(table, "frequency") for table in tables)
    descriptions = list(dict.fromkeys(sla[0].description for sla in found if sla is not None and sla[0].description))
    return f"Fraîcheur : {' '.join(descriptions)}" if descriptions else None


def instructions(data_context: DataContext) -> str:
    """The instructions of the space, from the data product document and the contracts."""
    data_product = _data_product(data_context)
    tables = _tables(data_context)
    document = data_product.document
    description = document.description if document else {}
    deprecated = [
        table.name
        for table in tables
        if table.contract.document is not None and table.contract.document.status in DEPRECATED
    ]
    paragraphs = [
        data_product.description,
        f"Usage : {description['usage']}" if description.get("usage") else None,
        f"Limites : {description['limitations']}" if description.get("limitations") else None,
        _keys(tables),
        f"Tables dépréciées, à n'utiliser que si la question les nomme : {', '.join(deprecated)}."
        if deprecated
        else None,
        "Les codes dont la description de la colonne ne liste pas les valeurs sont décrits dans la table "
        "domaine_valeur : la description de chaque colonne de code nomme son domaine de valeur, par exemple "
        f"(domaine de valeur {ACTIVITY_DOMAIN}). Joignez domaine_valeur sur domaine = '{ACTIVITY_DOMAIN}' "
        "et code = la colonne, et montrez la description du code plutôt que le code.",
        *_quality(tables),
        _classified(tables),
        _frequency(tables),
    ]
    return "\n\n".join(paragraph.strip() for paragraph in paragraphs if paragraph)


def top_activities_sql(data_context: DataContext) -> str:
    """The most frequent economic activities of the enterprises, described by their value domain."""
    context = _context(data_context)
    data_product = _data_product(data_context)
    enterprise = data_product.get_table("entreprise")
    domains = data_product.get_table("domaine_valeur")
    return (
        f"SELECT d.{_column(domains, 'description')} AS activite, count(*) AS entreprises\n"
        f"FROM {quote_name(_name(context.full_table_name(enterprise)))} AS e\n"
        f"JOIN {quote_name(_name(context.full_table_name(domains)))} AS d\n"
        f"  ON d.{_column(domains, 'domaine')} = {string_literal(ACTIVITY_DOMAIN)} "
        f"AND d.{_column(domains, 'code')} = e.{_column(enterprise, 'code_premiere_activite')}\n"
        f"GROUP BY d.{_column(domains, 'description')}\n"
        "ORDER BY entreprises DESC\n"
        "LIMIT 10"
    )


def error_rate_sql(data_context: DataContext) -> str | None:
    """The share of the rows of each table with a quality error, in percent."""
    context = _context(data_context)
    selects = []
    for table in _tables(data_context):
        settings = table.settings(QualitySettings)
        if not settings.enabled:
            continue
        errors = quote_identifier(resolved(settings.errors_column))
        selects.append(
            f"SELECT {string_literal(table.name)} AS table_name, "
            f"round(try_divide(100 * count_if(coalesce(size({errors}), 0) > 0), count(*)), 2) AS pourcentage_erreurs\n"
            f"FROM {quote_name(_name(context.full_table_name(table)))}"
        )
    return "\nUNION ALL\n".join(selects) + "\nORDER BY table_name" if selects else None


def examples(data_context: DataContext) -> list[dict[str, Any]]:
    """The example questions of the space, with their SQL."""
    found = [(TOP_ACTIVITIES_QUESTION, top_activities_sql(data_context))]
    error_rate = error_rate_sql(data_context)
    if error_rate:
        found.append((ERROR_RATE_QUESTION, error_rate))
    return [
        {"id": item_id(_EXAMPLE_SQL, index), "question": [question], "sql": lines(sql)}
        for index, (question, sql) in enumerate(found)
    ]


def build(data_context: DataContext) -> dict[str, Any]:
    """The Genie space of the data product: its title, description and serialized configuration."""
    context = _context(data_context)
    data_product = _data_product(data_context)
    document = data_product.document
    title = document.name if document and document.name else data_product.qualified_name
    space = {
        "version": VERSION,
        "config": {
            "sample_questions": [
                {"id": item_id(_SAMPLE_QUESTION, index), "question": [question]}
                for index, question in enumerate(SAMPLE_QUESTIONS)
            ]
        },
        "data_sources": {"tables": [table_source(context, table) for table in _tables(data_context)]},
        "instructions": {
            "text_instructions": [{"id": item_id(_TEXT_INSTRUCTION, 0), "content": lines(instructions(data_context))}],
            "example_question_sqls": examples(data_context),
            "join_specs": join_specs(data_context),
        },
    }
    return {"title": title, "description": data_product.description, "serialized_space": space}


def render(data_context: DataContext | None = None) -> str:
    """Returns the serialized Genie space, as committed."""
    data_context = data_context or DataContext.discover(PRODUCTS)
    return json.dumps(build(data_context), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    SPACE.write_text(render(), encoding="utf-8", newline="\n")
    print(f"Wrote {SPACE}")
