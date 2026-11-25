"""Tests of the metric views of the data products, generated from their contracts, without Spark nor workspace.

The tests read what they expect from the contracts and their configurations: changing a
contract does not need changing the tests.
"""

from pathlib import Path

import pytest

from cds import DataContext, DataProduct, ServiceContext
from cds.metric_views import build_metric_views
from cds.semantics import table_measures

PRODUCTS = Path(__file__).resolve().parents[1] / "products"


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


@pytest.fixture(scope="module")
def context(data_context: DataContext) -> ServiceContext:
    return ServiceContext(spark=None, environment="nprod", data_context=data_context)


@pytest.fixture(scope="module", params=["donnees_qc.req", "donnees_qc.repertoire_municipalites"])
def data_product(data_context: DataContext, request: pytest.FixtureRequest) -> DataProduct:
    return data_context.get_data_product(request.param)


def test_each_table_with_measures_has_a_metric_view(data_product: DataProduct, context: ServiceContext) -> None:
    views = {view.table.name: view for view in build_metric_views(data_product, context)}

    assert views
    for table in data_product.tables:
        if table_measures(table):
            # Named in French, like the history tables.
            assert views[table.name].name == f"{table.name}_indicateurs"
        else:
            assert table.name not in views


def test_the_measures_are_documented_in_french(data_product: DataProduct, context: ServiceContext) -> None:
    """Genie and the users read the business name and description of each measure."""
    for view in build_metric_views(data_product, context):
        for measure in view.definition["measures"]:
            assert measure.get("display_name") and measure.get("comment"), f"{view.name}.{measure['name']}"


def test_the_restricted_columns_are_not_dimensions(data_product: DataProduct, context: ServiceContext) -> None:
    """The deployment masks them: a dimension would let Genie group on masked values."""
    tables = {context.full_table_name(table): table for table in data_product.tables}
    for view in build_metric_views(data_product, context):
        aliases = {"source": view.table}
        aliases.update({join["name"]: tables[join["source"]] for join in view.definition.get("joins", [])})
        expressions = " ".join(field["expr"] for field in view.definition["fields"])
        for alias, table in aliases.items():
            for prop in table.columns:
                if prop.classification == "restricted":
                    assert f"{alias}.`{prop.column}`" not in expressions, f"{view.name}: {table.name}.{prop.column}"
