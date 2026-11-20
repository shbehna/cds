import pytest
from conftest import MakeProject

from cds import ConfigurationError, ContractError, DataContext, MetadataSettings, Table
from cds.services.tags import (
    MANAGED_TAGS_PROPERTY,
    TableTags,
    discover_domain,
    discover_domains,
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


DISCOVER_CONFIG = (
    "from cds import GroupConfig, MetadataSettings\n"
    "config = GroupConfig(name='sales', metadata=MetadataSettings(discover_domain=True))\n"
)


def with_subdomain(subdomain: str, domain: str | None = "sales") -> str:
    """The customer contract with a domain, or none, and a subdomain custom property."""
    lines = [f"    domain: {domain}\n"] if domain else []
    lines += ["    customProperties:\n", f"      - {{property: subdomain, value: {subdomain}}}\n"]
    return CUSTOMER.replace("    domain: sales\n", "".join(lines))


def test_no_discover_domain_by_default(make_project: MakeProject) -> None:
    table = customer(make_project, contract=with_subdomain("Orders"))

    assert discover_domain(table) is None
    assert "sales/Orders" not in expected_tags(table).table


def test_discover_domain(make_project: MakeProject) -> None:
    table = customer(make_project, DISCOVER_CONFIG)

    # The governed tag of the domain, without value; the domain field tag stays.
    assert discover_domain(table) == "sales"
    assert expected_tags(table).table["sales"] == ""
    assert expected_tags(table).table["domain"] == "sales"


def test_discover_subdomain(make_project: MakeProject) -> None:
    contract = with_subdomain('"Ventes au détail"', domain="Économie")
    table = customer(make_project, DISCOVER_CONFIG, contract)

    # The tag of a subdomain is its path under its domain.
    assert discover_domain(table) == "Économie/Ventes au détail"
    tags = expected_tags(table).table
    assert tags["Économie/Ventes au détail"] == ""
    assert "Économie" not in tags
    # A subdomain can itself be a path.
    nested = customer(make_project, DISCOVER_CONFIG, with_subdomain("Retail/Online"))
    assert discover_domain(nested) == "sales/Retail/Online"


def test_no_discover_domain_without_domain(make_project: MakeProject) -> None:
    contract = CUSTOMER.replace("    domain: sales\n", "")

    assert discover_domain(customer(make_project, DISCOVER_CONFIG, contract)) is None


def test_discover_domain_in_some_environments(make_project: MakeProject) -> None:
    config = DISCOVER_CONFIG.replace("discover_domain=True", "discover_domain=True, discover_environments=['prod']")
    table = customer(make_project, config)

    assert discover_domain(table, "prod") == "sales"
    assert discover_domain(table, "nprod") is None
    assert "sales" not in expected_tags(table, "nprod").table
    assert expected_tags(table, "prod").table["sales"] == ""
    # Without environment, the domain is validated as in every environment.
    assert discover_domain(table) == "sales"
    assert discover_domains([table], "nprod") == []


@pytest.mark.parametrize("environments", ["prod", ["prod", ""], [1]])
def test_the_discover_environments_are_validated(environments: object) -> None:
    with pytest.raises(ConfigurationError, match="discover_environments must be a list of environments"):
        MetadataSettings(discover_environments=environments)  # type: ignore[arg-type]


def test_no_discover_domain_without_tags(make_project: MakeProject) -> None:
    config = DISCOVER_CONFIG.replace("discover_domain=True", "discover_domain=True, tags=False")

    assert discover_domain(customer(make_project, config)) is None


@pytest.mark.parametrize(
    ("domain", "subdomain", "message"),
    [
        (None, "Orders", "the subdomain 'Orders' needs the domain of the contract"),
        ("sales", "a/b/c/d/e", "has more than 5 levels"),
        ("sales", "x" * 250, "is longer than 255 characters"),
        ("sales", '"Orders/"', "invalid Discover domain 'sales/Orders/': a tag key cannot be empty"),
        ("sales", "point-of-sale", "invalid Discover domain 'sales/point-of-sale': .* contains '-'"),
        ("sales", "12", "the subdomain custom property must be a string"),
    ],
)
def test_invalid_discover_domain(make_project: MakeProject, domain: str | None, subdomain: str, message: str) -> None:
    table = customer(make_project, DISCOVER_CONFIG, with_subdomain(subdomain, domain=domain))

    with pytest.raises(ContractError, match=message):
        discover_domain(table)
    # cds validate reports it.
    assert len(tag_problems(table)) == 1


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
