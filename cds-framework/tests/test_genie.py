"""Tests of the Genie spaces generated from the contracts (cds.genie), without Spark."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
from conftest import PROJECT, MakeProject

from cds import ConfigurationError, DataContext, DataProduct, GenieExample, GenieSettings, ServiceContext
from cds.cli import main
from cds.genie import build_space, error_rate_sql, joins

ID = re.compile(r"^[0-9a-f]{32}$")

GENIE_CONFIG = """
    from cds import DataProductConfig, GenieExample, GenieSettings, QualitySettings
    from cds.genie import error_rate_sql


    def open_orders(service_context, data_product):
        table = service_context.full_table_name(data_product.get_table("order"))
        return f"SELECT count(*) FROM {table} WHERE status = 'open'"


    config = DataProductConfig(
        name="orders",
        description="Orders of the shop.",
        quality=QualitySettings(criticality={"foreign_key": "warn"}),
        genie=GenieSettings(
            enabled=True,
            instructions=["Amounts are in euros."],
            sample_questions=["Which customers order the most?", "How many orders are open?"],
            examples=[
                GenieExample("How many orders are open?", open_orders),
                GenieExample("Which tables have errors?", error_rate_sql),
                GenieExample("How many customers?", "SELECT count(*) FROM customer"),
            ],
            masked_classifications=["restricted"],
        ),
    )
"""

CUSTOMER_CLASSIFIED = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.customer
    version: 1.0.0
    status: deprecated
    schema:
      - name: customer
        description: Customers of the shop.
        properties:
          - name: customer_id
            physicalType: STRING
            primaryKey: true
          - name: email
            physicalType: STRING
            classification: restricted
          - name: segment
            physicalType: STRING
            classification: internal
            enum:
              - { value: B2B, label: Business }
              - { value: B2C, label: Consumer }
"""

INVOICE = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.invoice
    version: 1.0.0
    schema:
      - name: invoice
        properties:
          - name: order_id
            physicalType: BIGINT
            primaryKey: true
          - name: customer_id
            physicalType: STRING
            relationships:
              - to: customer.customer_id
          - name: customer_id_billed
            physicalType: STRING
          - name: product_id
            physicalType: STRING
        relationships:
          # The same relationship as that of the property: a single join.
          - from: customer_id
            to: customer.customer_id
          # A second join to customer, with a dotted source.
          - from: invoice.customer_id_billed
            to: customer.customer_id
          # A table of another data product: not a table of the space.
          - from: product_id
            to: catalog.product.product_id
"""

PRODUCT = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.catalog.product
    version: 1.0.0
    schema:
      - name: product
        properties:
          - name: product_id
            physicalType: STRING
            primaryKey: true
"""


def genie_project(make_project: MakeProject, **files: str) -> DataContext:
    return DataContext.discover(
        make_project({**PROJECT, "sales/orders/orders_config.py": GENIE_CONFIG, **files}),
    )


@pytest.fixture
def data_context(make_project: MakeProject) -> DataContext:
    return genie_project(make_project)


def space_of(data_context: DataContext, environment: str = "dev") -> dict[str, Any]:
    context = ServiceContext(spark=None, environment=environment, data_context=data_context)
    return build_space(data_context.get_data_product("sales.orders"), context)


def text(value: list[str]) -> str:
    return "".join(value)


def test_the_space_has_the_tables_of_the_data_product(data_context: DataContext) -> None:
    space = space_of(data_context, "prod")

    assert space["title"] == "sales.orders"
    assert space["description"] == "Orders of the shop."
    assert space["serialized_space"]["version"] == 2
    tables = space["serialized_space"]["data_sources"]["tables"]
    assert [table["identifier"] for table in tables] == ["prod.sales_orders.customer", "prod.sales_orders.order"]


def test_the_joins_are_the_foreign_keys(data_context: DataContext) -> None:
    [join] = space_of(data_context)["serialized_space"]["instructions"]["join_specs"]

    assert join == {
        "id": "04000000000000000000000000000000",
        "left": {"identifier": "dev.sales_orders.order", "alias": "order"},
        "right": {"identifier": "dev.sales_orders.customer", "alias": "customer"},
        "sql": ["`order`.`customer_id` = `customer`.`customer_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
    }


def test_the_joins_of_the_relationships_of_a_schema_object(make_project: MakeProject) -> None:
    data_context = genie_project(
        make_project,
        **{"sales/orders/invoice/invoice.yaml": INVOICE, "sales/catalog/product/product.yaml": PRODUCT},
    )
    context = ServiceContext(spark=None, environment="dev", data_context=data_context)
    found = joins(data_context.get_data_product("sales.orders"), context)

    invoice = [(join.columns, join.alias, join.cardinality) for join in found if join.table.name == "invoice"]
    # Declared twice, the relationship of customer_id is one join. The billed customer is a
    # second join to customer, under its own alias. The product is not in the data product.
    assert invoice == [
        (("customer_id",), "customer", "MANY_TO_ONE"),
        (("customer_id_billed",), "customer_billed", "MANY_TO_ONE"),
    ]


def test_a_join_on_the_key_of_both_tables_is_one_to_one(make_project: MakeProject) -> None:
    detail = """
        apiVersion: v3.2.0
        kind: DataContract
        id: sales.orders.detail
        version: 1.0.0
        schema:
          - name: detail
            properties:
              - name: customer_id
                physicalType: STRING
                primaryKey: true
                relationships:
                  - to: customer.customer_id
              - name: referrer
                physicalType: STRING
                relationships:
                  - to: order.status
    """
    data_context = genie_project(make_project, **{"sales/orders/detail/detail.yaml": detail})
    context = ServiceContext(spark=None, environment="dev", data_context=data_context)

    found = {
        join.alias: join.cardinality
        for join in joins(data_context.get_data_product("sales.orders"), context)
        if join.table.name == "detail"
    }

    # order.status is not a key: a detail row may match several orders.
    assert found == {"customer": "ONE_TO_ONE", "order_referrer": "MANY_TO_MANY"}


def test_the_codes_are_described_with_their_labels(make_project: MakeProject) -> None:
    data_context = genie_project(make_project, **{"sales/orders/customer/customer.yaml": CUSTOMER_CLASSIFIED})
    tables = space_of(data_context)["serialized_space"]["data_sources"]["tables"]
    configs = {
        (table["identifier"], config["column_name"]): text(config["description"])
        for table in tables
        for config in table.get("column_configs", [])
    }

    assert configs == {
        ("dev.sales_orders.customer", "segment"): "Values: B2B (Business), B2C (Consumer).",
        ("dev.sales_orders.order", "status"): "Values: open, closed.",
    }


def test_the_instructions_come_from_the_contracts_and_the_settings(make_project: MakeProject) -> None:
    data_context = genie_project(make_project, **{"sales/orders/customer/customer.yaml": CUSTOMER_CLASSIFIED})
    [instruction] = space_of(data_context)["serialized_space"]["instructions"]["text_instructions"]
    content = text(instruction["content"])

    assert content.startswith(
        "Orders of the shop.\n\nKeys of the tables: customer (customer_id); order (order_id, line)."
    )
    assert "Deprecated tables, to use only when a question names them: customer." in content
    assert "A row is in error when size(_errors) > 0." in content
    # Only the masked classifications: the internal segment is readable.
    assert "do not filter nor group on them. customer.email." in content
    assert "segment" not in content.split("masked")[1]
    assert content.endswith("\n\nAmounts are in euros.")


def test_the_keys_and_the_service_levels(make_project: MakeProject) -> None:
    no_key = PROJECT["sales/orders/customer/customer.yaml"].replace("            primaryKey: true\n", "")
    defaults = """
        slaProperties:
          - property: errorRate
            value: 2
            unit: percent
          - property: frequency
            value: 1
            unit: d
            description: Loaded every night.
    """
    data_context = genie_project(
        make_project,
        **{"sales/orders/customer/customer.yaml": no_key, "sales/orders/orders.odcs.yaml": defaults},
    )
    content = text(space_of(data_context)["serialized_space"]["instructions"]["text_instructions"][0]["content"])

    assert "customer has no declared key: its rows may not be unique" in content
    assert "The service level allows at most 2 % of the rows of each table in error." in content
    assert "Frequency of the loads: Loaded every night." in content


def test_the_error_rate_needs_the_sla_of_every_table(make_project: MakeProject) -> None:
    customer = PROJECT["sales/orders/customer/customer.yaml"].replace(
        "    schema:",
        "    slaProperties:\n      - property: errorRate\n        value: 1\n        unit: percent\n    schema:",
    )
    data_context = genie_project(make_project, **{"sales/orders/customer/customer.yaml": customer})
    content = text(space_of(data_context)["serialized_space"]["instructions"]["text_instructions"][0]["content"])

    assert "service level" not in content, "the order table has no errorRate SLA"


def test_the_examples_and_sample_questions(data_context: DataContext) -> None:
    space = space_of(data_context)["serialized_space"]
    examples = {
        text(example["question"]): text(example["sql"]) for example in space["instructions"]["example_question_sqls"]
    }

    assert examples["How many orders are open?"] == "SELECT count(*) FROM dev.sales_orders.order WHERE status = 'open'"
    assert examples["How many customers?"] == "SELECT count(*) FROM customer"
    assert examples["Which tables have errors?"].startswith("SELECT 'customer' AS table_name, round(try_divide(")
    # The questions of the settings, then those of the examples, without duplicates.
    assert [text(question["question"]) for question in space["config"]["sample_questions"]] == [
        "Which customers order the most?",
        "How many orders are open?",
        "Which tables have errors?",
        "How many customers?",
    ]


def test_the_lists_are_sorted_and_their_ids_valid(make_project: MakeProject) -> None:
    """The Genie API rejects unsorted lists and ids other than 32 lowercase hexadecimal digits."""
    data_context = genie_project(
        make_project,
        **{"sales/orders/invoice/invoice.yaml": INVOICE, "sales/catalog/product/product.yaml": PRODUCT},
    )
    space = space_of(data_context)["serialized_space"]
    instructions = space["instructions"]

    for items in (space["config"]["sample_questions"], *instructions.values()):
        ids = [item["id"] for item in items]
        assert ids == sorted(ids)
        assert all(ID.match(item_id) for item_id in ids)
    together = [item["id"] for items in instructions.values() for item in items]
    assert len(together) == len(set(together)), "the ids of the instructions, examples and joins are unique together"
    identifiers = [table["identifier"] for table in space["data_sources"]["tables"]]
    assert identifiers == sorted(identifiers)


def test_the_sql_of_an_example_must_be_a_string(make_project: MakeProject) -> None:
    config = GENIE_CONFIG.replace('"SELECT count(*) FROM customer"', "lambda: 42")
    data_context = genie_project(make_project, **{"sales/orders/orders_config.py": config})

    with pytest.raises(ConfigurationError, match="the SQL of the example 'How many customers\\?' must be a string"):
        space_of(data_context)


def test_the_error_rate_needs_a_checked_table(make_project: MakeProject) -> None:
    config = GENIE_CONFIG.replace('criticality={"foreign_key": "warn"}', "enabled=False")
    data_context = genie_project(make_project, **{"sales/orders/orders_config.py": config})
    context = ServiceContext(spark=None, environment="dev", data_context=data_context)
    data_product: DataProduct = data_context.get_data_product("sales.orders")

    with pytest.raises(ConfigurationError, match="no table is checked by the quality service"):
        error_rate_sql(data_product, context)
    # Without the error rate example, the space has no quality instruction.
    config = config.replace('GenieExample("Which tables have errors?", error_rate_sql),', "")
    data_context = genie_project(make_project, **{"sales/orders/orders_config.py": config})
    content = text(space_of(data_context)["serialized_space"]["instructions"]["text_instructions"][0]["content"])
    assert "quality" not in content


def test_the_settings_are_validated() -> None:
    with pytest.raises(ConfigurationError, match=r"GenieSettings.sample_questions must be a list of strings"):
        GenieSettings(sample_questions="How many?")
    with pytest.raises(ConfigurationError, match=r"GenieSettings.examples must be GenieExample objects"):
        GenieSettings(examples=[("How many?", "SELECT 1")])  # type: ignore[list-item]
    assert GenieExample("How many?", "SELECT 1").sql == "SELECT 1"


def test_a_data_product_has_no_genie_space_by_default(project: Path) -> None:
    data_product = DataContext.discover(project).get_data_product("sales.orders")

    assert data_product.settings(GenieSettings).enabled is False


def test_cli_genie(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    root = make_project({**PROJECT, "sales/orders/orders_config.py": GENIE_CONFIG})

    assert main(["genie", str(root), "--data-product", "sales.orders", "--environment", "nprod"]) == 0
    space = json.loads(capsys.readouterr().out)
    assert space["serialized_space"]["data_sources"]["tables"][0]["identifier"] == "nprod.sales_orders.customer"

    assert main(["validate", str(root)]) == 0
    assert "+ Genie space: 2 table(s), 1 join(s), 3 example(s)" in capsys.readouterr().out
