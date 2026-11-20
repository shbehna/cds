"""Genie spaces of the data products, generated from their contracts.

A Genie space answers questions in natural language by writing SQL on tables. Genie reads
the comments of the tables and columns in Unity Catalog, which the metadata service sets
from the contracts. The space of a data product adds what the comments do not say:

- its tables (``data_sources``);
- their joins, from the ``foreignKey`` relationships of the contracts (``join_specs``);
- the values of the codes and their labels, from the ``enum`` of the properties, in the
  description of their columns (``column_configs``);
- instructions: the purpose, usage and limitations of the data product, the keys of the
  tables, the deprecated contracts, the quality results of each row and the error rate
  of the service levels, the masked STRING columns, and the frequency of the loads, followed by
  the ``instructions`` of ``GenieSettings`` (``text_instructions``);
- the ``sample_questions`` and ``examples`` of ``GenieSettings``.

``build_space`` returns the title, description and serialized configuration of the space
(``serialized_space``, version 2 of the Genie API, whose lists must be sorted), with the
tables named in the environment of the service context.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from cds.config import GenieExample, GenieSettings, QualitySettings, resolved
from cds.context import ServiceContext
from cds.data_context import DataProduct, Table
from cds.errors import CdsError, ConfigurationError, ContractError
from cds.injection import ContractInjection
from cds.odcs import Relationship, SchemaProperty
from cds.services.checks import build_checks, configured_checks
from cds.sla import percentage, table_sla
from cds.sql import quote_identifier, quote_name, string_literal

# Version of the serialized space of the Genie API.
VERSION = 2
FOREIGN_KEY = "foreignKey"
# Statuses of the contracts whose tables should not be used.
DEPRECATED = ("deprecated", "retired")
# Type of the columns a column mask on strings can mask.
MASKABLE_TYPE = "STRING"

# Category of each kind of item, the first byte of its id: the ids of the instructions,
# examples and joins must be unique together, and those of the questions too.
_SAMPLE_QUESTION = 1
_TEXT_INSTRUCTION = 2
_EXAMPLE_SQL = 3
_JOIN = 4


@dataclass(frozen=True)
class Join:
    """A join of a table of the data product to the table its foreign key references.

    Attributes:
        table: The table declaring the relationship.
        target: The referenced table.
        columns: The columns of ``table``.
        target_columns: The referenced columns of ``target``, in the same order.
        alias: Alias of ``target`` in the join.
        cardinality: Relationship of the rows: ``MANY_TO_ONE``, ``ONE_TO_ONE``,
            ``ONE_TO_MANY`` or ``MANY_TO_MANY``.
    """

    table: Table
    target: Table
    columns: tuple[str, ...]
    target_columns: tuple[str, ...]
    alias: str
    cardinality: str


def item_id(category: int, index: int) -> str:
    """Id of an item of a space: 32 lowercase hexadecimal digits, sorted by category, then index."""
    return f"{category:02x}{index:030x}"


def lines(text: str) -> list[str]:
    """A text as the Genie API stores it: a list of lines, each ending with its line break."""
    split = text.split("\n")
    return [line + "\n" for line in split[:-1]] + split[-1:]


def _tables(data_product: DataProduct, context: ServiceContext) -> list[Table]:
    """The tables of a data product, sorted by their full name, as the Genie API requires."""
    return sorted(data_product.tables, key=context.full_table_name)


def checked_tables(data_product: DataProduct, context: ServiceContext) -> list[Table]:
    """The tables of a data product to which the quality service adds its result columns.

    The service skips a table without checks, inferred or configured: it has no result
    columns. Checks computed at run time count as checks.
    """
    checked = []
    for table in _tables(data_product, context):
        if not table.settings(QualitySettings).enabled:
            continue
        dynamic = callable(getattr(table.contract.config, "quality_checks", None))
        inferred = build_checks(table, context, include_configured=False).checks
        if dynamic or inferred or configured_checks(table, context):
            checked.append(table)
    return checked


def column_description(prop: SchemaProperty) -> str | None:
    """The description of a column, with the values of its ``enum`` and their labels."""
    values = ", ".join(f"{value.value} ({value.label})" if value.label else str(value.value) for value in prop.enum)
    parts = [prop.description, f"Values: {values}." if values else None]
    return " ".join(part for part in parts if part) or None


def table_source(table: Table, context: ServiceContext) -> dict[str, Any]:
    """A table of the space, with the descriptions of its coded columns.

    The other columns are described by their comments in Unity Catalog.
    """
    source: dict[str, Any] = {"identifier": context.full_table_name(table)}
    configs = [
        {"column_name": prop.column, "description": lines(description)}
        for prop in sorted(table.columns, key=lambda prop: prop.column)
        if prop.enum and (description := column_description(prop))
    ]
    if configs:
        source["column_configs"] = configs
    return source


def _source_column(table: Table, reference: str, context: ServiceContext) -> str:
    """The column of a table designated by the source of a relationship of its schema object.

    Raises:
        ContractError: If the reference is unknown or designates a column of another table.
    """
    if "." not in reference:
        prop = table.get_column(reference)
        if prop is None:
            raise ContractError(f"{table.qualified_name}: the relationship source {reference!r} is unknown.")
        return prop.column
    found, column = context.data_context.resolve_reference(reference, table)
    if found is not table:
        raise ContractError(
            f"{table.qualified_name}: the relationship source {reference!r} must be a property of the table."
        )
    return column


def _unique(table: Table, columns: Sequence[str]) -> bool:
    """Whether columns identify the rows of a table: its primary key, or a unique column."""
    if table.primary_key and sorted(columns) == sorted(table.primary_key):
        return True
    prop = table.get_column(columns[0]) if len(columns) == 1 else None
    return prop is not None and prop.unique


def _alias(target: Table, columns: Sequence[str], target_columns: Sequence[str], origin: Table) -> str:
    """Alias of the referenced table of a join.

    The table keeps its name when the join uses its columns under the same names, as
    ``customer`` for ``customer_id``; otherwise its name gets the suffix of the columns:
    ``customer_billed`` for ``customer_id_billed``.
    """
    if list(columns) == list(target_columns) and target is not origin:
        return target.name
    suffixes = [
        column.removeprefix(f"{target_column}_") if column.startswith(f"{target_column}_") else column
        for column, target_column in zip(columns, target_columns, strict=True)
    ]
    return f"{target.name}_{'_'.join(suffixes)}"


def joins(data_product: DataProduct, context: ServiceContext) -> list[Join]:
    """The joins of the tables of a data product, from the ``foreignKey`` relationships of their contracts.

    A relationship declared on a property and again on its schema object is one join. A
    relationship to a table of another data product is not a join of the space, which
    only has the tables of its data product.

    Raises:
        ContractError: If a relationship references several tables, or has an unknown source.
        ConfigurationError: If two joins of a table get the same alias.
    """
    found: list[Join] = []
    for table in _tables(data_product, context):
        relationships: list[tuple[tuple[str, ...], Relationship]] = [
            ((prop.column,), rel) for prop in table.columns for rel in prop.relationships
        ]
        relationships += [
            (tuple(_source_column(table, ref, context) for ref in rel.source), rel)
            for rel in table.schema_object.relationships
        ]
        seen: set[tuple[tuple[str, ...], str, tuple[str, ...]]] = set()
        aliases = {table.name}
        for columns, relationship in relationships:
            if relationship.type != FOREIGN_KEY:
                continue
            targets = [context.data_context.resolve_reference(ref, table) for ref in relationship.target]
            if len({target.qualified_name for target, _ in targets}) != 1:
                raise ContractError(f"{table.qualified_name}: a relationship must reference a single table.")
            target = targets[0][0]
            target_columns = tuple(column for _, column in targets)
            key = (columns, target.qualified_name, target_columns)
            if target.data_product is not data_product or key in seen:
                continue
            seen.add(key)
            alias = _alias(target, columns, target_columns, table)
            if alias in aliases:
                raise ConfigurationError(f"{table.qualified_name}: two joins use the alias {alias!r}.")
            aliases.add(alias)
            many = "ONE" if _unique(table, columns) else "MANY"
            one = "ONE" if _unique(target, target_columns) else "MANY"
            found.append(Join(table, target, columns, target_columns, alias, f"{many}_TO_{one}"))
    return found


def join_specs(data_product: DataProduct, context: ServiceContext) -> list[dict[str, Any]]:
    """The joins of the space, as the Genie API describes them."""
    specs = []
    for index, join in enumerate(joins(data_product, context)):
        condition = " AND ".join(
            f"{quote_identifier(join.table.name)}.{quote_identifier(column)} = "
            f"{quote_identifier(join.alias)}.{quote_identifier(target_column)}"
            for column, target_column in zip(join.columns, join.target_columns, strict=True)
        )
        specs.append(
            {
                "id": item_id(_JOIN, index),
                "left": {"identifier": context.full_table_name(join.table), "alias": join.table.name},
                "right": {"identifier": context.full_table_name(join.target), "alias": join.alias},
                "sql": [condition, f"--rt=FROM_RELATIONSHIP_TYPE_{join.cardinality}--"],
            }
        )
    return specs


def _keys(tables: Sequence[Table]) -> str:
    keys = [
        f"{table.name} ({', '.join(table.primary_key)})"
        if table.primary_key
        else f"{table.name} has no declared key: its rows may not be unique"
        for table in tables
    ]
    return f"Keys of the tables: {'; '.join(keys)}."


def _quality(checked: Sequence[Table]) -> list[str]:
    if not checked:
        return []
    columns = {
        (resolved(settings.errors_column), resolved(settings.warnings_column))
        for settings in (table.settings(QualitySettings) for table in checked)
    }
    if len(columns) != 1:
        raise ConfigurationError("The quality result columns of the tables of a Genie space must be the same.")
    [(errors, warnings)] = columns
    text = [
        f"The quality checks write their results in each row: {errors} for the errors, {warnings} for the "
        "warnings. They are arrays of results, with the name of the check (name), its message (message) and "
        f"its columns (columns). A row is in error when size({errors}) > 0."
    ]
    slas = [sla for sla in (table_sla(table, "errorRate") for table in checked) if sla is not None]
    rates = {percentage(prop) for prop, _ in slas}
    if len(rates) == 1 and len(slas) == len(checked):
        text.append(f"The service level allows at most {rates.pop():g} % of the rows of each table in error.")
    return text


def _masked(tables: Sequence[Table], classifications: Sequence[str]) -> str | None:
    masked = [
        f"{table.name}.{prop.column}"
        for table in tables
        for prop in table.columns
        if prop.classification is not None
        and prop.classification in classifications
        and prop.data_type.upper() == MASKABLE_TYPE
    ]
    if not masked:
        return None
    return f"These columns are masked (****) for most users: do not filter nor group on them. {', '.join(masked)}."


def _frequency(tables: Sequence[Table]) -> str | None:
    found = (table_sla(table, "frequency") for table in tables)
    descriptions = list(dict.fromkeys(sla[0].description for sla in found if sla is not None and sla[0].description))
    return f"Frequency of the loads: {' '.join(descriptions)}" if descriptions else None


def instructions(data_product: DataProduct, context: ServiceContext) -> str:
    """The instructions of the space: generated from the contracts, then those of the settings."""
    settings = data_product.settings(GenieSettings)
    tables = _tables(data_product, context)
    description = data_product.document.description if data_product.document else {}
    deprecated = [
        table.name
        for table in tables
        if table.contract.document is not None and (table.contract.document.status or "").lower() in DEPRECATED
    ]
    paragraphs = [
        data_product.description,
        f"Usage: {description['usage']}" if description.get("usage") else None,
        f"Limitations: {description['limitations']}" if description.get("limitations") else None,
        _keys(tables),
        f"Deprecated tables, to use only when a question names them: {', '.join(deprecated)}." if deprecated else None,
        *_quality(checked_tables(data_product, context)),
        _masked(tables, resolved(settings.masked_classifications)),
        _frequency(tables),
        *resolved(settings.instructions),
    ]
    return "\n\n".join(paragraph.strip() for paragraph in paragraphs if paragraph and paragraph.strip())


def error_rate_sql(data_product: DataProduct, context: ServiceContext) -> str:
    """SQL of the share of the rows of each table with a quality error, in percent.

    A helper for the ``examples`` of ``GenieSettings``: the tables checked by the quality
    service (see ``checked_tables``), named in the environment of the context.

    Raises:
        ConfigurationError: If no table of the data product is checked.
    """
    selects = []
    for table in checked_tables(data_product, context):
        errors = quote_identifier(resolved(table.settings(QualitySettings).errors_column))
        selects.append(
            f"SELECT {string_literal(table.name)} AS table_name, "
            f"round(try_divide(100 * count_if(coalesce(size({errors}), 0) > 0), count(*)), 2) AS error_rate\n"
            f"FROM {quote_name(context.full_table_name(table))}"
        )
    if not selects:
        raise ConfigurationError(f"{data_product.qualified_name}: no table is checked by the quality service.")
    return "\nUNION ALL\n".join(selects) + "\nORDER BY table_name"


def run_time_examples(data_product: DataProduct, context: ServiceContext) -> list[str]:
    """The questions of the examples whose SQL needs a Spark session, which the context lacks.

    ``build_space`` leaves them out: without Spark, as in ``cds validate``, their SQL is
    only computed at run time.
    """
    if context.has_spark:
        return []
    return [example.question for example in _examples(data_product) if _needs_spark(example, context)]


def _examples(data_product: DataProduct) -> Sequence[GenieExample]:
    return resolved(data_product.settings(GenieSettings).examples)


def _needs_spark(example: GenieExample, context: ServiceContext) -> bool:
    return callable(example.sql) and ContractInjection(context).needs_spark(example.sql)


def examples(data_product: DataProduct, context: ServiceContext) -> list[dict[str, Any]]:
    """The example questions of the space, with their SQL.

    Without a Spark session, the examples whose SQL needs one are left out (see
    ``run_time_examples``).

    Raises:
        ConfigurationError: If the SQL of an example is not a string, or its function fails.
    """
    injection = ContractInjection(context, data_product=data_product)
    found: list[tuple[str, str]] = []
    for example in _examples(data_product):
        if not context.has_spark and _needs_spark(example, context):
            continue
        try:
            sql = injection.call(example.sql) if callable(example.sql) else example.sql
        except CdsError:
            raise
        except Exception as exc:
            raise ConfigurationError(
                f"{data_product.qualified_name}: the SQL of the example {example.question!r} failed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if not isinstance(sql, str) or not sql.strip():
            raise ConfigurationError(
                f"{data_product.qualified_name}: the SQL of the example {example.question!r} must be a string."
            )
        found.append((example.question, sql.strip()))
    return [
        {"id": item_id(_EXAMPLE_SQL, index), "question": [question], "sql": lines(sql)}
        for index, (question, sql) in enumerate(found)
    ]


def sample_questions(data_product: DataProduct) -> list[dict[str, Any]]:
    """The questions suggested to the users: those of the settings, then those of the examples."""
    settings = data_product.settings(GenieSettings)
    questions = [*resolved(settings.sample_questions), *(example.question for example in resolved(settings.examples))]
    return [
        {"id": item_id(_SAMPLE_QUESTION, index), "question": [question]}
        for index, question in enumerate(dict.fromkeys(questions))
    ]


def title(data_product: DataProduct) -> str:
    """The title of the space: that of the settings, else the name of the ODPS document, else the qualified name."""
    configured = data_product.settings(GenieSettings).title
    if configured:
        return configured
    document = data_product.document
    return document.name if document is not None and document.name else data_product.qualified_name


def build_space(data_product: DataProduct, context: ServiceContext) -> dict[str, Any]:
    """The Genie space of a data product: its ``title``, ``description`` and ``serialized_space``.

    The ``serialized_space`` is a mapping: the Genie API takes it serialized as a JSON
    string.
    """
    space = {
        "version": VERSION,
        "config": {"sample_questions": sample_questions(data_product)},
        "data_sources": {"tables": [table_source(table, context) for table in _tables(data_product, context)]},
        "instructions": {
            "text_instructions": [
                {"id": item_id(_TEXT_INSTRUCTION, 0), "content": lines(instructions(data_product, context))}
            ],
            "example_question_sqls": examples(data_product, context),
            "join_specs": join_specs(data_product, context),
        },
    }
    return {"title": title(data_product), "description": data_product.description, "serialized_space": space}
