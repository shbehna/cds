"""Tests of the quality dashboard of req, generated from its contracts, without Spark nor workspace."""

import re
from pathlib import Path
from typing import Any

import pytest

from cds import DashboardSettings, DataContext, ServiceContext
from cds.dashboard import build_dashboard

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
TABLES = {
    "continuation_transformation",
    "domaine_valeur",
    "entreprise",
    "etablissement",
    "fusion_scission",
    "nom_entreprise",
}


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


@pytest.fixture(scope="module")
def dashboard(data_context: DataContext) -> dict[str, Any]:
    context = ServiceContext(spark=None, environment="nprod", data_context=data_context)
    built: dict[str, Any] = build_dashboard(data_context.get_data_product("donnees_qc.req"), context)
    return built


def sql(dashboard: dict[str, Any], name: str) -> str:
    [dataset] = [item for item in dashboard["serialized_dashboard"]["datasets"] if item["name"] == name]
    return "".join(dataset["queryLines"])


def test_req_has_a_dashboard(data_context: DataContext, dashboard: dict[str, Any]) -> None:
    assert data_context.get_data_product("donnees_qc.req").settings(DashboardSettings).enabled
    assert dashboard["title"] == "REQ data quality"


def test_the_dashboard_reads_every_table_of_the_environment(dashboard: dict[str, Any]) -> None:
    tables = set(re.findall(r"FROM `nprod`\.`donnees_qc_req`\.`(\w+)`", sql(dashboard, "occurrences")))

    assert tables == TABLES


def test_rows_are_identified_by_their_primary_key(dashboard: dict[str, Any]) -> None:
    query = sql(dashboard, "occurrences")

    assert (
        "to_json(named_struct('numero_entreprise', `numero_entreprise`, 'numero_etablissement', "
        "`numero_etablissement`), map('ignoreNullFields', 'false')) AS row_key" in query
    )
    # fusion_scission has no primary key: its rows are identified by all their columns,
    # except the restricted addresses, which the dashboard would show unmasked.
    assert "'numero_entreprise_liee', `numero_entreprise_liee`" in query
    assert "adresse_ligne" not in query
