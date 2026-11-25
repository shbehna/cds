"""Tests of the Pages of the data products, generated from their contracts, without Spark nor workspace.

The tests read what they expect from the contracts and their configurations: changing a
contract does not need changing the tests.
"""

from pathlib import Path

import pytest

from cds import DataContext, DataProduct, PagesSettings, ServiceContext
from cds.pages import build_pages, title_conflicts
from cds.services.tags import discover_domain

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
# The data products with Pages.
DATA_PRODUCTS = sorted(
    data_product.qualified_name
    for data_product in DataContext.discover(PRODUCTS).data_products
    if data_product.settings(PagesSettings).enabled
)
INACTIVE = ("deprecated", "retired")


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


@pytest.fixture(scope="module", params=DATA_PRODUCTS)
def data_product(data_context: DataContext, request: pytest.FixtureRequest) -> DataProduct:
    return data_context.get_data_product(request.param)


@pytest.fixture(scope="module")
def nprod(data_context: DataContext) -> ServiceContext:
    return ServiceContext(spark=None, environment="nprod", data_context=data_context)


@pytest.fixture(scope="module")
def prod(data_context: DataContext) -> ServiceContext:
    return ServiceContext(spark=None, environment="prod", data_context=data_context)


def test_the_data_products_have_pages() -> None:
    assert DATA_PRODUCTS == ["donnees_qc.repertoire_municipalites", "donnees_qc.req"]


def test_each_active_table_has_a_page_with_a_business_name(data_product: DataProduct, prod: ServiceContext) -> None:
    pages = {page.key: page for page in build_pages(data_product, prod)}
    for table in data_product.tables:
        status = (table.contract.document.status or "").lower() if table.contract.document else ""
        page = pages.get(f"table:{table.name}")
        if status in INACTIVE:
            assert page is None, table.qualified_name
            continue
        assert page is not None, table.qualified_name
        # The title is the business name of the table, in French.
        assert page.display_name == table.schema_object.business_name, table.qualified_name
        assert page.domain == discover_domain(table)


def test_each_code_list_has_a_business_name(data_product: DataProduct, prod: ServiceContext) -> None:
    """Without business name, the Page of a code list would be titled by the name of its column."""
    pages = build_pages(data_product, prod)
    columns = {
        (table.name, prop.column): prop
        for page in pages
        for table in page.tables
        for prop in table.columns
        if prop.enum
    }
    for (table, column), prop in columns.items():
        assert prop.business_name, f"{data_product.qualified_name}.{table}.{column}"


def test_the_pages_are_published_in_prod_only(
    data_product: DataProduct, nprod: ServiceContext, prod: ServiceContext
) -> None:
    assert {page.state for page in build_pages(data_product, prod)} == {"PUBLISHED"}
    assert {page.state for page in build_pages(data_product, nprod)} == {"DRAFT"}


def test_the_titles_are_unique_in_their_domains(data_context: DataContext, prod: ServiceContext) -> None:
    pages = [page for name in DATA_PRODUCTS for page in build_pages(data_context.get_data_product(name), prod)]

    assert title_conflicts(pages) == []
