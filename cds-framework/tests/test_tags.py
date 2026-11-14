import pytest
from conftest import MakeProject

from cds import ContractError, DataContext, Table
from cds.services.tags import (
    MANAGED_TAGS_PROPERTY,
    TableTags,
    expected_tags,
    managed_keys,
    managed_property,
    parse_tag,
    tag_changes,
    tag_problems,
)

CUSTOMER = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.customer
    version: 1.0.0
    status: Active
    domain: sales
    dataProduct: sales.orders
    tags: [demo, "source: crm"]
    schema:
      - name: customer
        tags: [master_data]
        properties:
          - name: customer_id
            physicalType: STRING
          - name: email
            physicalType: STRING
            classification: restricted
            tags: ["pii:email"]
"""


def customer(make_project: MakeProject, group_config: str | None = None, contract: str = CUSTOMER) -> Table:
    files = {"sales/orders/customer/customer.yaml": contract}
    if group_config:
        files["sales/sales_config.py"] = group_config
    return DataContext.discover(make_project(files)).get_data_product("sales.orders").get_table("customer")


@pytest.mark.parametrize(
    ("tag", "expected"),
    [("demo", ("demo", "")), ("source: crm ", ("source", "crm")), ("url:https://x", ("url", "https://x"))],
)
def test_parse_tag(tag: str, expected: tuple[str, str]) -> None:
    assert parse_tag(tag, "t") == expected


@pytest.mark.parametrize(("tag", "message"), [("open-data", "contains '-'"), (":x", "cannot be empty")])
def test_parse_tag_rejects_invalid_keys(tag: str, message: str) -> None:
    with pytest.raises(ContractError, match=message):
        parse_tag(tag, "t")


def test_expected_tags(make_project: MakeProject) -> None:
    tags = expected_tags(customer(make_project))

    assert tags.table == {
        "domain": "sales",
        "data_product": "sales.orders",
        "demo": "",
        "source": "crm",
        "master_data": "",
    }
    assert tags.columns == {"email": {"pii": "email", "classification": "restricted"}}


def test_certification_follows_the_status(make_project: MakeProject) -> None:
    config = (
        "from cds import GroupConfig, MetadataSettings\n"
        "config = GroupConfig(name='sales', metadata=MetadataSettings(certification={'active': 'certified'}))\n"
    )
    assert expected_tags(customer(make_project, config)).table["system.certification_status"] == "certified"

    deprecated = CUSTOMER.replace("status: Active", "status: deprecated")
    table = customer(make_project, config, deprecated)

    assert "system.certification_status" not in expected_tags(table).table


def test_field_tags_can_be_disabled(make_project: MakeProject) -> None:
    config = (
        "from cds import GroupConfig, MetadataSettings\n"
        "config = GroupConfig(name='sales', metadata=MetadataSettings(field_tags={'domain': '', 'tenant': 'tenant'}))\n"
    )

    tags = expected_tags(customer(make_project, config)).table

    assert "domain" not in tags and "tenant" not in tags
    assert tags["data_product"] == "sales.orders"


def test_tag_problems(make_project: MakeProject) -> None:
    config = (
        "from cds import GroupConfig, MetadataSettings\n"
        "config = GroupConfig(name='sales', metadata=MetadataSettings(classifications=['public']))\n"
    )

    assert tag_problems(customer(make_project)) == []
    assert tag_problems(customer(make_project, config)) == [
        "the classification 'restricted' of email is not one of public"
    ]
    invalid = customer(make_project, contract=CUSTOMER.replace("[master_data]", "[master-data]"))
    assert "invalid tag 'master-data'" in tag_problems(invalid)[0]


def test_tag_changes_only_remove_the_managed_tags() -> None:
    changes = tag_changes(
        expected={"a": "1", "b": "2"},
        actual={"a": "1", "b": "old", "c": "x", "d": "y"},
        managed=["b", "c", "e"],
    )

    assert changes.set == {"b": "2"}
    assert changes.unset == ("c",)
    assert not tag_changes({"a": "1"}, {"a": "1"}, ["a"])


def test_managed_keys_round_trip() -> None:
    value = managed_property(TableTags({"b": "", "a": "1"}, {"Email": {"pii": "x"}, "empty": {}}))

    assert value == '{"columns":{"Email":["pii"]},"table":["a","b"]}'
    assert managed_keys({MANAGED_TAGS_PROPERTY: value}) == ({"a", "b"}, {"email": {"pii"}})
    assert managed_keys({}) == (set(), {})
    assert managed_keys({MANAGED_TAGS_PROPERTY: "not json"}) == (set(), {})
    assert managed_keys({MANAGED_TAGS_PROPERTY: '["a"]'}) == (set(), {})
