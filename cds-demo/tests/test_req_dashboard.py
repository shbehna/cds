"""Tests of the quality dashboard of req, generated from its contracts, without Spark nor workspace.

The tests read what they expect from the contracts and their configurations: changing a
contract does not need changing the tests.
"""

import re
from pathlib import Path
from typing import Any

import pytest

from cds import DashboardSettings, DataContext, DataProduct, ServiceContext
from cds.config import resolved
from cds.dashboard import build_dashboard

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
DATA_PRODUCT = "donnees_qc.req"
# The rows of a table in the query of the occurrences: its name, then the key of its rows.
ROW_KEY = re.compile(r"SELECT '(\w+)' AS table_name, to_json\(named_struct\((.*?)\), map\('ignoreNullFields'")


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


@pytest.fixture(scope="module")
def data_product(data_context: DataContext) -> DataProduct:
    return data_context.get_data_product(DATA_PRODUCT)


@pytest.fixture(scope="module")
def context(data_context: DataContext) -> ServiceContext:
    return ServiceContext(spark=None, environment="nprod", data_context=data_context)


@pytest.fixture(scope="module")
def dashboard(data_product: DataProduct, context: ServiceContext) -> dict[str, Any]:
    built: dict[str, Any] = build_dashboard(data_product, context)
    return built


def sql(dashboard: dict[str, Any], name: str) -> str:
    [dataset] = [item for item in dashboard["serialized_dashboard"]["datasets"] if item["name"] == name]
    return "".join(dataset["queryLines"])


def test_req_has_a_dashboard(data_product: DataProduct, dashboard: dict[str, Any]) -> None:
    settings = data_product.settings(DashboardSettings)
    name = data_product.document.name if data_product.document else None

    assert settings.enabled
    assert dashboard["title"] == (settings.title or f"{name} data quality")


def test_the_dashboard_reads_every_table_of_the_environment(
    data_product: DataProduct, context: ServiceContext, dashboard: dict[str, Any]
) -> None:
    query = sql(dashboard, "occurrences")

    for table in data_product.tables:
        assert "FROM `{}`".format(context.full_table_name(table).replace(".", "`.`")) in query, table.name


def test_rows_are_identified_by_their_primary_key_or_their_shown_columns(
    data_product: DataProduct, dashboard: dict[str, Any]
) -> None:
    keys = {
        name: re.findall(r"'(\w+)', `\w+`", struct) for name, struct in ROW_KEY.findall(sql(dashboard, "occurrences"))
    }
    shown = resolved(data_product.settings(DashboardSettings).shown_classifications) or []

    assert set(keys) == {table.name for table in data_product.tables}
    for table in data_product.tables:
        if table.primary_key:
            assert keys[table.name] == list(table.primary_key), table.name
        else:
            # Without primary key, all their columns but those the dashboard may not show.
            expected = [prop.column for prop in table.columns if prop.classification in (None, *shown)]
            assert keys[table.name] == expected, table.name
