"""Tests of the Genie space of the demo, without Spark nor workspace."""

import json
import re
from typing import Any

import genie_space
import pytest

from cds import ConfigurationError, DataContext

ID = re.compile(r"^[0-9a-f]{32}$")
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
    return DataContext.discover(genie_space.PRODUCTS)


@pytest.fixture(scope="module")
def document(data_context: DataContext) -> dict[str, Any]:
    rendered: dict[str, Any] = json.loads(genie_space.render(data_context))
    return rendered


@pytest.fixture(scope="module")
def space(document: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = document["serialized_space"]
    return serialized


def text(value: list[str]) -> str:
    return "".join(value)


def test_the_committed_space_is_up_to_date(data_context: DataContext) -> None:
    committed = genie_space.SPACE.read_text(encoding="utf-8")

    assert committed == genie_space.render(data_context), (
        "The Genie space does not match the contracts: run uv run python cds-demo/genie/genie_space.py"
    )


def test_the_space_is_described_by_the_data_product_document(document: dict[str, Any]) -> None:
    assert document["title"] == "Registre des entreprises du Québec"
    assert document["description"].startswith("Registre des entreprises du Québec : les entreprises")
    assert document["serialized_space"]["version"] == 2


def test_the_space_reads_every_table_of_the_data_product(space: dict[str, Any]) -> None:
    identifiers = [table["identifier"] for table in space["data_sources"]["tables"]]

    # In the catalog and schema of the naming conventions: the deployment sets the environment.
    assert identifiers == [f"${{environment}}.donnees_qc_req.{table}" for table in TABLES]


def test_the_lists_are_sorted_and_their_ids_valid(space: dict[str, Any]) -> None:
    """The Genie API rejects unsorted lists and ids other than 32 lowercase hexadecimal digits."""
    tables = space["data_sources"]["tables"]
    assert [table["identifier"] for table in tables] == sorted(table["identifier"] for table in tables)
    for table in tables:
        columns = [config["column_name"] for config in table.get("column_configs", [])]
        assert columns == sorted(columns), table["identifier"]
    instructions = space["instructions"]
    groups = {
        "questions": space["config"]["sample_questions"],
        "instructions": [
            *instructions["text_instructions"],
            *instructions["example_question_sqls"],
            *instructions["join_specs"],
        ],
    }
    for items in (space["config"]["sample_questions"], *instructions.values()):
        ids = [item["id"] for item in items]
        assert ids == sorted(ids)
    for name, items in groups.items():
        ids = [item["id"] for item in items]
        assert all(ID.match(item_id) for item_id in ids), name
        assert len(ids) == len(set(ids)), f"the ids of the {name} must be unique together"
    assert len(instructions["text_instructions"]) == 1, "a space has at most one text instruction"


def test_the_joins_are_the_foreign_keys_of_the_contracts(space: dict[str, Any]) -> None:
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
    # The linked enterprise of a merger is a second join to entreprise, under its own alias.
    assert joins[("fusion_scission", "entreprise_liee")] == [
        "`fusion_scission`.`numero_entreprise_liee` = `entreprise_liee`.`numero_entreprise`",
        "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
    ]
    for join in space["instructions"]["join_specs"]:
        assert join["right"]["identifier"] == "${environment}.donnees_qc_req.entreprise"


def test_the_codes_are_described_with_their_labels(space: dict[str, Any]) -> None:
    configs = {
        (table["identifier"].rsplit(".", 1)[1], config["column_name"]): text(config["description"])
        for table in space["data_sources"]["tables"]
        for config in table.get("column_configs", [])
    }

    # The columns with an enum: the others are described by their comment in Unity Catalog.
    assert set(configs) == {
        ("continuation_transformation", "code_type_changement"),
        ("entreprise", "code_forme_juridique"),
        ("entreprise", "code_intervalle_employes"),
        ("entreprise", "code_statut_immatriculation"),
        ("fusion_scission", "code_relation"),
        ("nom_entreprise", "code_statut_nom"),
        ("nom_entreprise", "code_type_nom"),
    }
    assert configs[("continuation_transformation", "code_type_changement")].endswith(
        "Valeurs : CONTN (Continuation), TRANS (Transformation)."
    )


def test_the_instructions_come_from_the_contracts(space: dict[str, Any]) -> None:
    instructions = text(space["instructions"]["text_instructions"][0]["content"])

    assert "Usage : Analyse des entreprises immatriculées au Québec." in instructions
    assert "Limites : Source : Registraire des entreprises du Québec" in instructions
    assert "etablissement (numero_entreprise, numero_etablissement)" in instructions
    assert "fusion_scission n'a pas de clé" in instructions
    assert "Tables dépréciées, à n'utiliser que si la question les nomme : fusion_scission." in instructions
    assert "Une ligne est en erreur si size(_errors) > 0." in instructions
    assert "au plus 1 % de lignes en erreur par table" in instructions
    assert "etablissement.adresse_ligne_1 (restricted)" in instructions
    assert "Fraîcheur : Le registre est publié deux fois par mois" in instructions


def test_the_examples_name_the_tables_of_the_environment(space: dict[str, Any]) -> None:
    examples = {
        text(example["question"]): text(example["sql"]) for example in space["instructions"]["example_question_sqls"]
    }

    assert set(examples) == {genie_space.TOP_ACTIVITIES_QUESTION, genie_space.ERROR_RATE_QUESTION}
    assert (
        "ON d.`domaine` = 'ACT_ECON' AND d.`code` = e.`code_premiere_activite`"
        in examples[genie_space.TOP_ACTIVITIES_QUESTION]
    )
    error_rate = examples[genie_space.ERROR_RATE_QUESTION]
    assert set(re.findall(r"FROM `\$\{environment\}`\.`donnees_qc_req`\.`(\w+)`", error_rate)) == set(TABLES)
    sample = [text(question["question"]) for question in space["config"]["sample_questions"]]
    assert set(examples) <= set(sample), "the examples are also shown as sample questions"


def test_an_example_on_an_unknown_column_fails(data_context: DataContext) -> None:
    enterprise = data_context.get_data_product(genie_space.DATA_PRODUCT).get_table("entreprise")

    with pytest.raises(ConfigurationError, match="code_inconnu"):
        genie_space._column(enterprise, "code_inconnu")
