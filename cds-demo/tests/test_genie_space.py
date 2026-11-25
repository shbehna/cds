"""Tests of the Genie spaces of the data products, generated from their contracts, without Spark nor workspace.

The tests read what they expect from the contracts and their configurations: changing a
contract does not need changing the tests.
"""

import re
from pathlib import Path
from typing import Any

import pytest

from cds import DataContext, DataProduct, GenieSettings, ServiceContext, Table
from cds.config import resolved
from cds.genie import build_space
from cds.metric_views import build_metric_views

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
# The data products with a Genie space.
DATA_PRODUCTS = sorted(
    data_product.qualified_name
    for data_product in DataContext.discover(PRODUCTS).data_products
    if data_product.settings(GenieSettings).enabled
)
# A condition of a join: `left`.`column` = `right`.`column`.
JOIN_CONDITION = re.compile(r"`(\w+)`\.`(\w+)` = `(\w+)`\.`(\w+)`")


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


@pytest.fixture(scope="module", params=DATA_PRODUCTS)
def data_product(data_context: DataContext, request: pytest.FixtureRequest) -> DataProduct:
    return data_context.get_data_product(request.param)


@pytest.fixture(scope="module")
def context(data_context: DataContext) -> ServiceContext:
    return ServiceContext(spark=None, environment="nprod", data_context=data_context)


@pytest.fixture(scope="module")
def document(data_product: DataProduct, context: ServiceContext) -> dict[str, Any]:
    return build_space(data_product, context)


@pytest.fixture(scope="module")
def space(document: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = document["serialized_space"]
    return serialized


@pytest.fixture(scope="module")
def tables(data_product: DataProduct, context: ServiceContext) -> dict[str, Table]:
    """The tables of the data product, by full name."""
    return {context.full_table_name(table): table for table in data_product.tables}


def text(value: list[str]) -> str:
    return "".join(value)


def test_the_data_products_have_a_genie_space() -> None:
    assert DATA_PRODUCTS == ["donnees_qc.repertoire_municipalites", "donnees_qc.req"]


def test_the_title_of_the_space(data_product: DataProduct, document: dict[str, Any]) -> None:
    settings = data_product.settings(GenieSettings)

    assert settings.enabled
    # The title of the settings, or the name of the ODPS document.
    assert document["title"] == (settings.title or (data_product.document and data_product.document.name))


def test_the_space_reads_every_table(
    space: dict[str, Any], tables: dict[str, Table], data_product: DataProduct, context: ServiceContext
) -> None:
    """The tables of the data product, and the metric views of their measures."""
    identifiers = [table["identifier"] for table in space["data_sources"]["tables"]]
    views = [context.full_table_name(view.table, view.name) for view in build_metric_views(data_product, context)]

    assert identifiers == sorted([*tables, *views])


def test_the_joins_use_columns_of_their_tables(space: dict[str, Any], tables: dict[str, Table]) -> None:
    for join in space["instructions"]["join_specs"]:
        left, right = join["left"], join["right"]
        condition, relationship = join["sql"]
        pairs = JOIN_CONDITION.findall(condition)
        assert pairs, condition
        for left_alias, left_column, right_alias, right_column in pairs:
            assert (left_alias, right_alias) == (left["alias"], right["alias"]), condition
            assert tables[left["identifier"]].get_column(left_column), condition
            assert tables[right["identifier"]].get_column(right_column), condition
        assert re.fullmatch(r"--rt=FROM_RELATIONSHIP_TYPE_\w+--", relationship)


def test_the_codes_are_described_with_their_labels(space: dict[str, Any], tables: dict[str, Table]) -> None:
    configs = {
        (table["identifier"], config["column_name"]): text(config["description"])
        for table in space["data_sources"]["tables"]
        for config in table.get("column_configs", [])
    }
    enums = {(name, prop.column): prop for name, table in tables.items() for prop in table.columns if prop.enum}

    assert set(enums) <= set(configs)
    for key, prop in enums.items():
        for value in prop.enum:
            described = f"{value.value} ({value.label})" if value.label else str(value.value)
            assert described in configs[key], key


def test_the_instructions(data_product: DataProduct, space: dict[str, Any], tables: dict[str, Table]) -> None:
    content = text(space["instructions"]["text_instructions"][0]["content"])
    settings = data_product.settings(GenieSettings)
    masked = resolved(settings.masked_classifications) or []

    assert content.startswith((data_product.description or "").strip())
    for table in tables.values():
        if not table.primary_key:
            assert f"{table.name} has no declared key" in content
        for prop in table.columns:
            if prop.classification in masked and prop.data_type.upper() == "STRING":
                assert f"{table.name}.{prop.column}" in content
    for instruction in resolved(settings.instructions) or []:
        assert instruction.strip() in content


def test_the_examples(space: dict[str, Any], tables: dict[str, Table]) -> None:
    examples = {
        text(example["question"]): text(example["sql"]) for example in space["instructions"]["example_question_sqls"]
    }
    questions = [text(question["question"]) for question in space["config"]["sample_questions"]]

    assert set(examples) <= set(questions)
    known = {name.replace(".", "`.`") for name in tables}
    for question, sql in examples.items():
        # The examples read the tables of the data product, in the environment of the space.
        read = set(re.findall(r"`(\w+`\.`\w+`\.`\w+)`", sql))
        assert read and read <= known, question
