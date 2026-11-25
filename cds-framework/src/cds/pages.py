"""Pages of the data products, generated from their contracts.

A Page is the governed definition of a business concept, such as an entity or a code
list, in a Databricks Discover domain. Genie Ontology, the context layer of the Genie
agents, takes the Pages as modeled knowledge, over what it learns from the queries and
dashboards. The Pages of a data product (``PagesSettings``) are:

- a Page for each table whose contract is neither deprecated nor retired: the description
  of the table, the purpose, usage and limitations of its data product, its key, its
  columns, its relationships (the ``foreignKey`` relationships of the contracts, see
  ``cds.genie.joins``), its service levels and its ``authoritativeDefinitions``;
- a Page for each code list (``PagesSettings.code_lists``): the codes, labels and
  descriptions of the ``enum`` of a column. The columns whose code lists have the same
  title share a Page: their codes must then be the same.

The title of a Page is the ``businessName`` of its schema object or property, else its
name, and its synonyms the ``synonyms`` custom property, a list of strings. The titles
of the published Pages of a Discover domain are unique.

A Page is in the Discover domain of its table (see ``cds.services.tags.discover_domain``):
a table without domain has none. It is published in the environments of
``MetadataSettings.discover_environments``, and a draft in the others, so that a test
environment does not publish a second definition of the same concept. Its text is the
documentation of the contracts: it holds no value of the data.

``build_pages`` returns the Pages of a data product, with the tables named in the
environment of the service context.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from cds.config import PagesSettings, resolved
from cds.context import ServiceContext
from cds.data_context import DataProduct, Table
from cds.errors import ContractError
from cds.genie import DEPRECATED, Join, column_description, joins
from cds.odcs import AuthoritativeDefinition, EnumValue, SchemaProperty
from cds.services.checks import ALLOWED_VALUES
from cds.services.tags import discover_domain
from cds.sla import percentage, table_sla

# Custom property of a schema object or a property holding the synonyms of its Page.
SYNONYMS_PROPERTY = "synonyms"
# Limits of the Pages API.
MAX_TITLE_LENGTH = 200
MAX_SYNONYMS = 50
MAX_SYNONYM_LENGTH = 200
PUBLISHED = "PUBLISHED"
DRAFT = "DRAFT"
TABLE_KEY = "table"
CODE_LIST_KEY = "code_list"


@dataclass(frozen=True)
class Page:
    """A Page of a data product.

    Attributes:
        key: Identity of the Page in its data product, stable across runs: ``table:<table>``
            or ``code_list:<domain>:<title in lowercase>``.
        domain: The governed tag of its Discover domain.
        display_name: Its title.
        description: Its summary.
        body: Its content, in Markdown.
        synonyms: The other names of the concept, sorted.
        published: Whether it is published, else a draft.
        tables: The tables it describes.
    """

    key: str
    domain: str
    display_name: str
    description: str | None
    body: str
    synonyms: tuple[str, ...] = ()
    published: bool = False
    tables: tuple[Table, ...] = field(default=(), compare=False, repr=False)

    @property
    def state(self) -> str:
        """The state of the Page in the Pages API: ``PUBLISHED`` or ``DRAFT``."""
        return PUBLISHED if self.published else DRAFT


@dataclass
class _CodeList:
    """The columns sharing the code list of a title, in a domain."""

    domain: str
    title: str
    values: tuple[EnumValue, ...]
    columns: list[tuple[Table, SchemaProperty]] = field(default_factory=list)


def synonyms(custom_properties: Mapping[str, Any], where: str) -> tuple[str, ...]:
    """The synonyms of a schema object or a property: its ``synonyms`` custom property, trimmed, sorted.

    Raises:
        ContractError: If it is not a list of strings, or exceeds the limits of the Pages API.
    """
    value = custom_properties.get(SYNONYMS_PROPERTY)
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence) or not all(isinstance(item, str) for item in value):
        raise ContractError(f"{where}: the {SYNONYMS_PROPERTY} custom property must be a list of strings.")
    return _checked_synonyms(value, where)


def _checked_synonyms(values: Iterable[str], where: str) -> tuple[str, ...]:
    found = sorted({value.strip() for value in values})
    for value in found:
        if not value or len(value) > MAX_SYNONYM_LENGTH:
            raise ContractError(
                f"{where}: a synonym must have from 1 to {MAX_SYNONYM_LENGTH} characters, got {value!r}."
            )
    if len(found) > MAX_SYNONYMS:
        raise ContractError(f"{where}: more than {MAX_SYNONYMS} synonyms.")
    return tuple(found)


def _title(business_name: str | None, name: str, where: str) -> str:
    """The title of a Page: a business name, else a name; a blank business name is no business name."""
    title = (business_name or "").strip() or name.strip()
    if not title or len(title) > MAX_TITLE_LENGTH:
        raise ContractError(
            f"{where}: the title {title!r} of its Page must have from 1 to {MAX_TITLE_LENGTH} characters."
        )
    return title


def _cell(text: str | None) -> str:
    """A text as a cell of a Markdown table: on one line, its pipes escaped."""
    return " ".join((text or "").split()).replace("|", "\\|")


def _code(text: str) -> str:
    return f"`{text}`"


def _code_cell(text: str) -> str:
    """A code as a cell of a Markdown table, such as a value of an ``enum``."""
    return _cell(_code(text))


def _product_title(data_product: DataProduct) -> str:
    document = data_product.document
    return document.name if document is not None and document.name else data_product.qualified_name


def _link(label: str, url: str) -> str:
    """A Markdown link: the brackets of its label escaped, the spaces and parentheses of its URL encoded."""
    text = _cell(label).replace("[", "\\[").replace("]", "\\]")
    target = url.strip().replace(" ", "%20").replace("(", "%28").replace(")", "%29")
    return f"[{text}]({target})"


def _references(definitions: Sequence[AuthoritativeDefinition]) -> str | None:
    if not definitions:
        return None
    items = [f"- {_link(item.description or item.type or item.url, item.url)}" for item in definitions]
    return "## References\n\n" + "\n".join(items)


def _active(table: Table) -> bool:
    document = table.contract.document
    return document is not None and (document.status or "").lower() not in DEPRECATED


def _allowed_values(table: Table, prop: SchemaProperty, context: ServiceContext) -> str | None:
    """Where the codes of a column checked against another table are, as its ``allowedValues`` reads them."""
    declaration = prop.custom_properties.get(ALLOWED_VALUES)
    if not isinstance(declaration, Mapping) or not isinstance(declaration.get("reference"), str):
        return None
    target, column = context.data_context.resolve_reference(declaration["reference"], table)
    source = f"the values of {_code(f'{context.full_table_name(target)}.{column}')}"
    row_filter = declaration.get("filter")
    return f"Codes: {source} where {_code(row_filter)}." if isinstance(row_filter, str) else f"Codes: {source}."


def _column_text(
    table: Table, prop: SchemaProperty, code_lists: Mapping[tuple[str, str], str], context: ServiceContext
) -> str:
    title = code_lists.get((table.name, prop.column))
    if title is not None:
        parts = [prop.description, f"Codes: see the Page *{title}*."]
    elif prop.enum:
        parts = [column_description(prop)]
    else:
        parts = [prop.description, _allowed_values(table, prop, context)]
    return " ".join(part for part in parts if part)


def _columns(table: Table, code_lists: Mapping[tuple[str, str], str], context: ServiceContext) -> str:
    named = any(prop.business_name for prop in table.columns)
    header = ["Column", *(["Name"] if named else []), "Type", "Description"]
    rows = [
        [
            _code_cell(prop.column),
            *([_cell(prop.business_name)] if named else []),
            _code_cell(prop.data_type),
            _cell(_column_text(table, prop, code_lists, context)),
        ]
        for prop in table.columns
    ]
    lines = [header, ["---"] * len(header), *rows]
    return "## Columns\n\n" + "\n".join(f"| {' | '.join(line)} |" for line in lines)


def _relationships(table: Table, found: Sequence[Join], titles: Mapping[str, str]) -> str | None:
    items = [
        f"- {', '.join(_code(column) for column in join.columns)}: "
        f"*{titles.get(join.target.name, join.target.name)}* "
        f"({', '.join(_code(f'{join.target.name}.{column}') for column in join.target_columns)}), "
        f"{join.cardinality.lower().replace('_', ' ')}"
        for join in found
        if join.table is table
    ]
    return "## Relationships\n\n" + "\n".join(items) if items else None


def _service_levels(table: Table) -> str | None:
    items = []
    frequency = table_sla(table, "frequency")
    if frequency is not None and frequency[0].description:
        items.append(f"- Frequency: {' '.join(frequency[0].description.split())}")
    error_rate = table_sla(table, "errorRate")
    if error_rate is not None:
        items.append(f"- At most {percentage(error_rate[0]):g} % of the rows fail the quality checks.")
    return "## Service levels\n\n" + "\n".join(items) if items else None


def _table_body(
    table: Table,
    context: ServiceContext,
    code_lists: Mapping[tuple[str, str], str],
    found: Sequence[Join],
    titles: Mapping[str, str],
) -> str:
    data_product = table.data_product
    description = data_product.document.description if data_product.document else {}
    key = (
        ", ".join(_code(column) for column in table.primary_key)
        if table.primary_key
        else "No declared key: the rows may not be unique."
    )
    sections = [
        f"Table {_code(context.full_table_name(table))} of the data product *{_product_title(data_product)}*.",
        table.description,
        f"## Data product\n\n{data_product.description}" if data_product.description else None,
        f"## Usage\n\n{description['usage']}" if description.get("usage") else None,
        f"## Limitations\n\n{description['limitations']}" if description.get("limitations") else None,
        f"## Key\n\n{key}",
        _columns(table, code_lists, context),
        _relationships(table, found, titles),
        _service_levels(table),
        _references(table.schema_object.authoritative_definitions),
    ]
    return "\n\n".join(section.strip() for section in sections if section and section.strip())


def _code_list_body(code_list: _CodeList, titles: Mapping[str, str]) -> str:
    columns = ", ".join(
        f"{_code(f'{table.name}.{prop.column}')} (*{titles[table.name]}*)" for table, prop in code_list.columns
    )
    described = any(value.description for value in code_list.values)
    header = ["Code", "Label", *(["Description"] if described else [])]
    rows = [
        [_code_cell(str(value.value)), _cell(value.label), *([_cell(value.description)] if described else [])]
        for value in code_list.values
    ]
    lines = [header, ["---"] * len(header), *rows]
    definitions = [item for _, prop in code_list.columns for item in prop.authoritative_definitions]
    sections = [
        f"Codes of the columns {columns}.",
        "\n".join(f"| {' | '.join(line)} |" for line in lines),
        _references(list(dict.fromkeys(definitions))),
    ]
    return "\n\n".join(section for section in sections if section)


def _code_lists(tables: Sequence[Table], domains: Mapping[str, str]) -> list[_CodeList]:
    """The code lists of the columns of tables, by domain and title.

    Raises:
        ContractError: If columns with code lists of the same title have different codes.
    """
    found: dict[tuple[str, str], _CodeList] = {}
    for table in tables:
        for prop in table.columns:
            if not prop.enum:
                continue
            where = f"{table.qualified_name}.{prop.column}"
            title = _title(prop.business_name, prop.column, where)
            domain = domains[table.name]
            code_list = found.setdefault((domain, title.casefold()), _CodeList(domain, title, prop.enum))
            if code_list.values != prop.enum:
                first, column = code_list.columns[0]
                raise ContractError(
                    f"{where}: its code list {title!r} has other codes than that of "
                    f"{first.qualified_name}.{column.column}: give them different business names."
                )
            code_list.columns.append((table, prop))
    return sorted(found.values(), key=lambda item: (item.domain, item.title.casefold()))


def build_pages(data_product: DataProduct, context: ServiceContext) -> list[Page]:
    """The Pages of a data product: those of its tables, then of their code lists.

    The tables without Discover domain, and those of the deprecated or retired contracts,
    have none.

    Raises:
        ContractError: If a title or synonyms are invalid, or two Pages of a domain have
            the same title.
    """
    tables = [
        table
        for table in sorted(data_product.tables, key=context.full_table_name)
        if _active(table) and discover_domain(table) is not None
    ]
    domains = {table.name: str(discover_domain(table)) for table in tables}
    published = {table.name: discover_domain(table, context.environment.id) is not None for table in tables}
    titles = {
        table.name: _title(table.schema_object.business_name, table.name, table.qualified_name) for table in tables
    }
    code_lists = _code_lists(tables, domains) if resolved(data_product.settings(PagesSettings).code_lists) else []
    listed = {(table.name, prop.column): item.title for item in code_lists for table, prop in item.columns}
    found = joins(data_product, context)
    pages = [
        Page(
            key=f"{TABLE_KEY}:{table.name}",
            domain=domains[table.name],
            display_name=titles[table.name],
            description=table.description,
            body=_table_body(table, context, listed, found, titles),
            synonyms=synonyms(table.schema_object.custom_properties, table.qualified_name),
            published=published[table.name],
            tables=(table,),
        )
        for table in tables
    ]
    for item in code_lists:
        first_table, first = item.columns[0]
        pages.append(
            Page(
                # Case-insensitive, as the grouping: the key does not depend on the first column.
                key=f"{CODE_LIST_KEY}:{item.domain}:{item.title.casefold()}",
                domain=item.domain,
                display_name=item.title,
                description=first.description,
                body=_code_list_body(item, titles),
                synonyms=_checked_synonyms(
                    (
                        synonym
                        for table, prop in item.columns
                        for synonym in synonyms(prop.custom_properties, f"{table.qualified_name}.{prop.column}")
                    ),
                    f"{first_table.qualified_name}.{first.column}",
                ),
                published=any(published[table.name] for table, _ in item.columns),
                tables=tuple(dict.fromkeys(table for table, _ in item.columns)),
            )
        )
    problems = title_conflicts(pages)
    if problems:
        raise ContractError(f"{data_product.qualified_name}: {'; '.join(problems)}.")
    return pages


def title_conflicts(pages: Iterable[Page]) -> list[str]:
    """The titles shared by several Pages of a domain, which the Pages API refuses once published."""
    seen: dict[tuple[str, str], Page] = {}
    problems = []
    for page in pages:
        other = seen.setdefault((page.domain, page.display_name.casefold()), page)
        if other is not page:
            problems.append(
                f"the Pages {_describe(other)} and {_describe(page)} have the same title {page.display_name!r} "
                f"in the Discover domain {page.domain!r}"
            )
    return problems


def _describe(page: Page) -> str:
    kind = "of the table" if page.key.startswith(f"{TABLE_KEY}:") else "of the code list of"
    return f"{kind} {', '.join(table.qualified_name for table in page.tables)}"
