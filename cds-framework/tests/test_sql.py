import pytest

from cds.odcs import SchemaProperty
from cds.sql import (
    create_table_statement,
    ddl_type,
    normalize_type,
    quote_identifier,
    quote_name,
    string_literal,
)


def test_quoting() -> None:
    assert quote_identifier("order") == "`order`"
    assert quote_identifier("a`b") == "`a``b`"
    assert quote_name("cat.sch.order") == "`cat`.`sch`.`order`"
    assert string_literal("l'entreprise \\ x") == "'l\\'entreprise \\\\ x'"


@pytest.mark.parametrize(
    ("contract_type", "table_type"),
    [
        ("INTEGER", "int"),
        ("Long", "bigint"),
        ("VARCHAR(10)", "string"),
        ("char(1)", "string"),
        ("DECIMAL", "decimal(10,0)"),
        ("decimal(5)", "decimal(5,0)"),
        ("DECIMAL(10, 2)", "decimal(10,2)"),
        ("NUMERIC(4,1)", "decimal(4,1)"),
        ("ARRAY<INTEGER>", "array<int>"),
        ("TIMESTAMP_LTZ", "timestamp"),
        ("timestamp_ntz", "timestamp_ntz"),
    ],
)
def test_normalize_type(contract_type: str, table_type: str) -> None:
    assert normalize_type(contract_type) == normalize_type(table_type) == table_type


def test_ddl_type_from_spark_json() -> None:
    data_type = {
        "type": "array",
        "elementType": {
            "type": "struct",
            "fields": [
                {"name": "function", "type": "string"},
                {"name": "columns", "type": {"type": "array", "elementType": "string"}},
                {"name": "user_metadata", "type": {"type": "map", "keyType": "string", "valueType": "integer"}},
            ],
        },
    }

    assert ddl_type(data_type) == (
        "ARRAY<STRUCT<`function`: STRING, `columns`: ARRAY<STRING>, `user_metadata`: MAP<STRING, INT>>>"
    )
    assert ddl_type("decimal(10,2)") == "DECIMAL(10,2)"


def test_create_table_statement() -> None:
    columns = [
        SchemaProperty(name="id", physical_type="BIGINT", description="The id"),
        SchemaProperty(name="day", logical_type="date"),
    ]

    statement = create_table_statement(
        "c.s.t",
        columns,
        description="It's a table",
        partition_columns=["day"],
        properties={"cds.contract.id": "x"},
        extra_columns=[("_row_hash", "STRING")],
    )

    assert statement == (
        "CREATE TABLE IF NOT EXISTS `c`.`s`.`t` (\n"
        "  `id` BIGINT COMMENT 'The id',\n"
        "  `day` DATE,\n"
        "  `_row_hash` STRING\n"
        ")\n"
        "USING DELTA\n"
        "PARTITIONED BY (`day`)\n"
        "COMMENT 'It\\'s a table'\n"
        "TBLPROPERTIES ('cds.contract.id' = 'x')"
    )
    assert create_table_statement("c.s.t", columns, replace=True).startswith("CREATE OR REPLACE TABLE `c`.`s`.`t`")
