from pathlib import Path
from typing import Any

import pytest

from cds import ContractError
from cds.odcs import DataContract, merge_documents


def contract(**overrides: object) -> dict[str, Any]:
    document: dict[str, Any] = {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": "c1",
        "version": "1.0.0",
        "schema": [
            {
                "name": "orders",
                "physicalName": "order_lines",
                "description": "Lines.",
                "properties": [
                    {"name": "line", "physicalType": "INT", "primaryKey": True, "primaryKeyPosition": 2},
                    {"name": "order_id", "logicalType": "integer", "primaryKey": True, "primaryKeyPosition": 1},
                    {"name": "label", "physicalName": "label_text"},
                    {"name": "day", "logicalType": "date", "partitioned": True, "partitionKeyPosition": -1},
                    {
                        "name": "status",
                        "logicalType": "string",
                        "required": True,
                        "enum": [{"value": "open", "label": "Open"}, "closed"],
                        "customProperties": [{"property": "owner", "value": "sales"}],
                        "quality": [{"metric": "nullValues", "mustBe": 0, "severity": "warning"}],
                    },
                ],
                "relationships": [{"from": ["order_id"], "to": ["header.id"]}],
            }
        ],
    }
    document.update(overrides)
    return document


def test_parses_tables_and_columns() -> None:
    parsed = DataContract.from_dict(contract(), source="c1.yaml")
    table = parsed.schema[0]

    assert parsed.id == "c1" and parsed.api_version == "v3.2.0"
    assert table.table_name == "order_lines"
    assert [prop.column for prop in table.properties] == ["line", "order_id", "label_text", "day", "status"]
    assert table.primary_key == ("order_id", "line")
    assert table.partition_columns == ("day",)
    label = table.get_property("LABEL_TEXT")
    assert label is not None and label.name == "label"
    assert [prop.data_type for prop in table.properties] == ["INT", "BIGINT", "STRING", "DATE", "STRING"]

    status = table.get_property("status")
    assert status is not None
    assert status.required and [value.value for value in status.enum] == ["open", "closed"]
    assert status.custom_properties == {"owner": "sales"}
    rule = status.quality[0]
    assert (rule.type, rule.metric, rule.thresholds, rule.severity) == (
        "library",
        "nullValues",
        {"mustBe": 0},
        "warning",
    )
    assert table.relationships[0].source == ("order_id",)
    assert table.relationships[0].target == ("header.id",)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"kind": "Other"}, "kind"),
        ({"apiVersion": "v2.2.2"}, "unsupported version"),
        ({"id": None}, "id: is required"),
        ({"schema": [{"name": "t", "properties": [{"name": "a"}, {"name": "A"}]}]}, "declared more than once"),
        ({"schema": [{"name": "t", "properties": [{"name": "a", "required": "yes"}]}]}, "must be true or false"),
        ({"schema": [{"name": "t", "properties": [{"name": "a", "logicalType": "object"}]}]}, "explicit physicalType"),
        (
            {"schema": [{"name": "t", "properties": [{"name": "a", "relationships": [{"to": ["x.a", "x.b"]}]}]}]},
            "single property",
        ),
        ({"schema": [{"name": "t", "relationships": [{"from": "a", "to": ["x.a", "x.b"]}]}]}, "same number"),
    ],
)
def test_invalid_contracts(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ContractError, match=message):
        DataContract.from_dict(contract(**overrides), source="c1.yaml")


def test_errors_locate_the_element() -> None:
    document = contract(schema=[{"name": "t", "properties": [{"physicalType": "INT"}]}])

    with pytest.raises(ContractError, match=r"c1\.yaml: schema\[0\]\.properties\[0\]\.name: is required"):
        DataContract.from_dict(document, source="c1.yaml")


def test_merge_documents_merges_mappings_and_replaces_lists() -> None:
    merged = merge_documents(
        {"tags": ["a"], "description": {"usage": "u", "purpose": "p"}},
        None,
        {"tags": ["b"], "description": {"purpose": "q"}},
    )

    assert merged == {"tags": ["b"], "description": {"usage": "u", "purpose": "q"}}


def test_from_file_applies_defaults(tmp_path: Path) -> None:
    path = tmp_path / "c1.yaml"
    path.write_text("apiVersion: v3.2.0\nkind: DataContract\nid: c1\nversion: 1.0.0\ntags: [own]\n", encoding="utf-8")

    parsed = DataContract.from_file(path, defaults={"tenant": "shop", "tags": ["default"]})

    assert parsed.tenant == "shop"
    assert parsed.tags == ("own",)


def test_from_file_reports_invalid_yaml(tmp_path: Path) -> None:
    path = tmp_path / "c1.yaml"
    path.write_text("- a list\n", encoding="utf-8")

    with pytest.raises(ContractError, match="YAML mapping"):
        DataContract.from_file(path)
