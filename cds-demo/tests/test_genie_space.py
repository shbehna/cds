"""Tests of the Genie space of req, generated from its contracts, without Spark nor workspace."""

from pathlib import Path
from typing import Any

import pytest

from cds import DataContext, GenieSettings, ServiceContext
from cds.genie import build_space

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
TABLES = [
    "continuation_transformation",
    "domaine_valeur",
    "entreprise",
    "etablissement",
    "fusion_scission",
    "nom_entreprise",
]


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


@pytest.fixture(scope="module")
def document(data_context: DataContext) -> dict[str, Any]:
    context = ServiceContext(spark=None, environment="nprod", data_context=data_context)
    return build_space(data_context.get_data_product("donnees_qc.req"), context)


@pytest.fixture(scope="module")
def space(document: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = document["serialized_space"]
    return serialized


def text(value: list[str]) -> str:
    return "".join(value)


def test_req_has_a_genie_space(data_context: DataContext, document: dict[str, Any]) -> None:
    assert data_context.get_data_product("donnees_qc.req").settings(GenieSettings).enabled
    # The name of the ODPS document.
    assert document["title"] == "Registre des entreprises du Québec"


def test_the_space_reads_every_table(space: dict[str, Any]) -> None:
    identifiers = [table["identifier"] for table in space["data_sources"]["tables"]]

    assert identifiers == [f"nprod.donnees_qc_req.{table}" for table in TABLES]


def test_the_joins_are_the_foreign_keys(space: dict[str, Any]) -> None:
    joins = {
        (join["left"]["alias"], join["right"]["alias"]): join["sql"] for join in space["instructions"]["join_specs"]
    }

    assert set(joins) == {
        ("continuation_transformation", "entreprise"),
        ("etablissement", "entreprise"),
        ("fusion_scission", "entreprise"),
        ("fusion_scission", "entreprise_liee"),
        ("nom_entreprise", "entreprise"),
    }
    assert joins[("fusion_scission", "entreprise_liee")] == [
        "`fusion_scission`.`numero_entreprise_liee` = `entreprise_liee`.`numero_entreprise`",
        "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
    ]


def test_the_codes_are_described_with_their_labels(space: dict[str, Any]) -> None:
    configs = {
        (table["identifier"].rsplit(".", 1)[1], config["column_name"]): text(config["description"])
        for table in space["data_sources"]["tables"]
        for config in table.get("column_configs", [])
    }

    assert len(configs) == 7, "the columns with an enum"
    assert configs[("continuation_transformation", "code_type_changement")].endswith(
        "Values: CONTN (Continuation), TRANS (Transformation)."
    )


def test_the_instructions(space: dict[str, Any]) -> None:
    content = text(space["instructions"]["text_instructions"][0]["content"])

    assert content.startswith("Registre des entreprises du Québec : les entreprises")
    assert "Usage: Analyse des entreprises immatriculées au Québec." in content
    assert "fusion_scission has no declared key" in content
    assert "Deprecated tables, to use only when a question names them: fusion_scission." in content
    assert "at most 1 % of the rows of each table in error" in content
    assert "etablissement.adresse_ligne_1" in content
    assert "Frequency of the loads: Le registre est publié deux fois par mois" in content
    # The value domains of req: the domain named by each column, not always ACT_ECON.
    assert "joignez domaine_valeur sur upper(trim(domaine)) = ce domaine" in content


def test_the_examples(space: dict[str, Any]) -> None:
    examples = {
        text(example["question"]): text(example["sql"]) for example in space["instructions"]["example_question_sqls"]
    }

    activities = examples["Quelles sont les 10 activités économiques les plus fréquentes des entreprises ?"]
    assert "FROM `nprod`.`donnees_qc_req`.`entreprise` AS e" in activities
    # The value domain of a few codes is not in capitals, which a quality check reports.
    assert "ON upper(trim(d.`domaine`)) = 'ACT_ECON' AND trim(d.`code`) = e.`code_premiere_activite`" in activities
    error_rate = examples["Quelle part des lignes de chaque table a une erreur de qualité ?"]
    assert error_rate.count("UNION ALL") == len(TABLES) - 1
    questions = [text(question["question"]) for question in space["config"]["sample_questions"]]
    assert len(questions) == 5
    assert set(examples) <= set(questions)
