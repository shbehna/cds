from pathlib import Path

import pytest
from conftest import CUSTOMER, MakeProject

from cds import ConfigurationError, ContractConfig, ContractError, DataContext, GroupConfig, QualitySettings
from cds.config import resolved


def test_discovers_the_layout(project: Path) -> None:
    context = DataContext.discover(project)

    [group] = context.groups
    assert group.name == "sales"
    assert isinstance(group.config, GroupConfig) and group.config.name == "sales"
    data_product = context.get_data_product("sales.orders")
    assert data_product.description == "Orders of the shop."
    assert [contract.name for contract in data_product.contracts] == ["customer", "order"]
    assert [table.qualified_name for table in context.tables] == ["sales.orders.customer", "sales.orders.order"]
    assert context.get_data_product("orders") is data_product


def test_contract_defaults_are_merged(project: Path) -> None:
    document = DataContext.discover(project).get_table("customer").contract.document

    assert document is not None
    assert document.tenant == "shop"
    assert document.tags == ("demo",)


def test_partial_contracts_are_merged_under_the_contracts(make_project: MakeProject) -> None:
    root = make_project(
        {
            "sales/sales.odcs.yaml": "tenant: shop\ntags: [group]\ndomain: group\n",
            "sales/orders/orders.odcs.yaml": (
                "tags: [data_product]\nslaProperties:\n  - property: latency\n    value: 4\n    unit: d\n"
            ),
            "sales/orders/customer/customer.yaml": CUSTOMER.replace(
                "    version: 1.0.0\n", "    version: 1.0.0\n    domain: contract\n"
            ),
        }
    )

    data_product = DataContext.discover(root).get_data_product("sales.orders")

    document = data_product.contracts[0].document
    assert document is not None
    assert (document.tenant, document.tags, document.domain) == ("shop", ("data_product",), "contract")
    assert [prop.property for prop in document.sla_properties] == ["latency"]
    assert data_product.contract_defaults["tenant"] == "shop"
    assert data_product.group.contract_defaults == {"tenant": "shop", "tags": ["group"], "domain": "group"}


@pytest.mark.parametrize("suffix", [".odcs.yaml", ".odcs.yml"])
def test_a_partial_contract_alone_defines_a_group(make_project: MakeProject, suffix: str) -> None:
    context = DataContext.discover(make_project({f"sales/sales{suffix}": "tenant: shop\n"}))

    [group] = context.groups
    assert (group.name, group.contract_defaults, group.data_products) == ("sales", {"tenant": "shop"}, [])


def test_a_partial_contract_without_content_defines_no_defaults(make_project: MakeProject) -> None:
    root = make_project(
        {"sales/sales.odcs.yaml": "# No defaults yet.\n", "sales/orders/customer/customer.yaml": CUSTOMER}
    )

    assert DataContext.discover(root).get_data_product("sales.orders").contract_defaults == {}


@pytest.mark.parametrize(
    ("files", "error", "message"),
    [
        (
            {
                "sales/sales.odcs.yaml": "tenant: shop\n",
                "sales/sales_config.py": (
                    "from cds import GroupConfig\n"
                    "config = GroupConfig(name='sales', contract_defaults={'tags': ['a']})\n"
                ),
            },
            ConfigurationError,
            "both define the contract defaults",
        ),
        ({"sales/orders/orders.odcs.yaml": "id: x\nversion: 1.0.0\n"}, ContractError, "cannot set id, version"),
        (
            {
                "sales/sales_config.py": (
                    "from cds import GroupConfig\n"
                    "config = GroupConfig(name='sales', contract_defaults={'version': '9.9.9'})\n"
                )
            },
            ContractError,
            "contract_defaults: a partial data contract cannot set version",
        ),
        ({"sales/orders/orders.odcs.yaml": "- tenant\n"}, ContractError, "must be a mapping"),
        ({"sales/orders/orders.odcs.yaml": "tenant: [\n"}, ContractError, "Cannot read the partial data contract"),
    ],
)
def test_invalid_partial_contracts(
    make_project: MakeProject, files: dict[str, str], error: type[Exception], message: str
) -> None:
    root = make_project({**files, "sales/orders/customer/customer.yaml": CUSTOMER})

    with pytest.raises(error, match=message):
        DataContext.discover(root)


def test_configuration_files_can_use_relative_imports(project: Path) -> None:
    config = DataContext.discover(project).get_table("order").contract.config

    assert isinstance(config, ContractConfig) and callable(config.transform)
    assert config.transform.__module__.endswith(".sales.orders.order.order_config")


def test_settings_are_inherited(project: Path) -> None:
    context = DataContext.discover(project)

    criticality = resolved(context.get_table("order").settings(QualitySettings).criticality)
    assert criticality["foreign_key"] == "warn"
    assert criticality["required"] == "error"


def test_folders_without_contract_are_ignored(make_project: MakeProject) -> None:
    root = make_project(
        {
            "sales/orders/customer/customer.yaml": CUSTOMER,
            "sales/orders/helpers/__init__.py": "",
            "sales/orders/notes/readme.txt": "",
            "external-doc/file.csv": "",
            "_private/x/y/y.yaml": CUSTOMER,
        }
    )

    context = DataContext.discover(root)

    assert [dp.qualified_name for dp in context.data_products] == ["sales.orders"]
    assert [contract.name for contract in context.contracts] == ["customer"]


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({"sales/sales_config.py": "x = 1\n"}, "must define a module-level 'config'"),
        (
            {"sales/sales_config.py": "from cds import GroupConfig\nconfig = GroupConfig(name='other')\n"},
            "must be 'sales'",
        ),
        (
            {"sales/sales_config.py": "from cds import DataProductConfig\nconfig = DataProductConfig(name='sales')\n"},
            "expected a GroupConfig",
        ),
        ({"sales/sales_config.py": "import missing_module\n"}, "Cannot import"),
        (
            {"sales/orders/customer/customer_config.py": "config = None\n"},
            "does not exist",
        ),
        ({"bad-name/bad-name_config.py": ""}, "Invalid name"),
        ({"bad-name/bad-name.odcs.yaml": ""}, "Invalid name"),
    ],
)
def test_invalid_layouts(make_project: MakeProject, files: dict[str, str], message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        DataContext.discover(make_project(files))


def test_a_config_class_is_instantiated(make_project: MakeProject) -> None:
    root = make_project(
        {
            "sales/sales_config.py": (
                "from cds import GroupConfig\n\nclass Sales(GroupConfig):\n    name = 'sales'\n\nconfig = Sales\n"
            ),
            "sales/orders/customer/customer.yaml": CUSTOMER,
        }
    )

    config = DataContext.discover(root).groups[0].config
    assert isinstance(config, GroupConfig) and config.name == "sales"


def test_duplicate_tables_in_a_data_product(make_project: MakeProject) -> None:
    root = make_project({"s/d/a/a.yaml": CUSTOMER, "s/d/b/b.yaml": CUSTOMER})

    with pytest.raises(ConfigurationError, match="declared by the contracts 'a' and 'b'"):
        DataContext.discover(root)


def test_groups_cannot_be_defined_twice(make_project: MakeProject) -> None:
    first = make_project({"s/d/a/a.yaml": CUSTOMER}, name="one")
    second = make_project({"s/e/a/a.yaml": CUSTOMER}, name="two")

    with pytest.raises(ConfigurationError, match="The group 's' is defined"):
        DataContext.discover([first, second])


def test_table_lookup_prefers_the_closest_table_and_rejects_ambiguity(make_project: MakeProject) -> None:
    root = make_project(
        {
            "s/first/customer/customer.yaml": CUSTOMER,
            "s/first/other/other.yaml": CUSTOMER.replace("name: customer\n", "name: other\n"),
            "s/second/customer/customer.yaml": CUSTOMER,
        }
    )
    context = DataContext.discover(root)
    origin = context.get_table("first.other")

    assert context.get_table("customer", origin=origin).qualified_name == "s.first.customer"
    assert context.get_table("s.second.customer").qualified_name == "s.second.customer"
    with pytest.raises(ConfigurationError, match="Ambiguous table 'customer'"):
        context.get_table("customer")
    with pytest.raises(ConfigurationError, match="Unknown table"):
        context.get_table("missing")


def test_resolve_references(project: Path) -> None:
    context = DataContext.discover(project)
    order = context.get_table("order")

    assert context.resolve_reference("customer.customer_id", order) == (context.get_table("customer"), "customer_id")
    assert context.resolve_reference("orders.customer.CUSTOMER_ID", order)[1] == "customer_id"
    with pytest.raises(ContractError, match="has no property 'unknown'"):
        context.resolve_reference("customer.unknown", order)
    with pytest.raises(ContractError, match="Unknown table"):
        context.resolve_reference("supplier.id", order)
    with pytest.raises(ContractError, match="other files"):
        context.resolve_reference("other.yaml#/schema/a/properties/b", order)


def test_resolve_id_references(make_project: MakeProject) -> None:
    contract = CUSTOMER.replace("- name: customer\n", "- name: customer\n        id: cust\n").replace(
        "- name: customer_id\n", "- name: customer_id\n            id: cid\n"
    )
    context = DataContext.discover(make_project({"s/d/customer/customer.yaml": contract}))
    table = context.get_table("customer")

    assert context.resolve_reference("schema/cust/properties/cid", table) == (table, "customer_id")
    with pytest.raises(ContractError, match="designates no property"):
        context.resolve_reference("schema/cust/properties/nope", table)
