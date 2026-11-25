"""Tests of the Pages generated from the contracts (cds.pages), without Spark."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

import pytest
from conftest import PROJECT, MakeProject

from cds import ConfigurationError, ContractError, DataContext, PagesSettings, ServiceContext
from cds.cli import main
from cds.odcs import AuthoritativeDefinition, DataContract
from cds.pages import Page, build_pages

GROUP = """
    from cds import GroupConfig, MetadataSettings

    config = GroupConfig(
        name="sales",
        contract_defaults={"domain": "Ventes"},
        metadata=MetadataSettings(discover_domain=True, discover_environments=["prod"]),
    )
"""

PRODUCT = """
    from cds import DataProductConfig, PagesSettings

    config = DataProductConfig(name="orders", description="Orders of the shop.", pages=PagesSettings(enabled=True))
"""

CUSTOMER = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.customer
    version: 1.0.0
    schema:
      - name: customer
        businessName: Client
        description: Customers of the shop.
        customProperties:
          - property: synonyms
            value: [" Buyer ", Account, Buyer]
        authoritativeDefinitions:
          - url: https://example.com/glossary/customer
            type: businessDefinition
            description: Glossary of the shop
        properties:
          - name: customer_id
            businessName: Customer number
            physicalType: STRING
            primaryKey: true
            description: Identifier | of the customer.
          - name: segment
            businessName: Segment
            physicalType: STRING
            description: Segment of the customer.
            enum:
              - { value: B2B, label: Business }
              - { value: B2C, label: Consumer }
"""

ORDER = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.order
    version: 1.0.0
    slaProperties:
      - property: frequency
        value: 1
        unit: d
        description: Loaded every day.
      - property: errorRate
        value: 2
        unit: percent
    schema:
      - name: order
        description: Orders.
        properties:
          - name: order_id
            physicalType: BIGINT
            primaryKey: true
          - name: customer_id
            physicalType: STRING
            relationships:
              - to: customer.customer_id
          - name: status
            businessName: Order status
            physicalType: STRING
            description: Status of the order.
            customProperties:
              - property: synonyms
                value: [State]
            enum:
              - { value: open, label: Open, description: Not shipped yet }
              - { value: closed, label: Closed }
          - name: customer_segment
            businessName: Segment
            physicalType: STRING
            enum:
              - { value: B2B, label: Business }
              - { value: B2C, label: Consumer }
          - name: channel
            physicalType: STRING
            customProperties:
              - property: allowedValues
                value: { reference: customer.segment, filter: "segment <> 'B2C'" }
"""

FILES = {
    "sales/sales_config.py": GROUP,
    "sales/orders/orders_config.py": PRODUCT,
    "sales/orders/customer/customer.yaml": CUSTOMER,
    "sales/orders/order/order.yaml": ORDER,
}


def pages_of(root: Path, environment: str = "prod") -> list[Page]:
    context = ServiceContext(spark=None, environment=environment, data_context=DataContext.discover(root))
    return build_pages(context.data_context.get_data_product("sales.orders"), context)


def by_key(pages: list[Page]) -> dict[str, Page]:
    return {page.key: page for page in pages}


def project(make_project: MakeProject, changes: Mapping[str, str] | None = None, name: str = "products") -> Path:
    return make_project({**FILES, **(changes or {})}, name=name)


def test_each_table_and_code_list_has_a_page(make_project: MakeProject) -> None:
    pages = pages_of(project(make_project))

    assert [page.key for page in pages] == [
        "table:customer",
        "table:order",
        "code_list:Ventes:order status",
        "code_list:Ventes:segment",
    ]
    customer = pages[0]
    assert (customer.display_name, customer.description) == ("Client", "Customers of the shop.")
    assert customer.domain == "Ventes"
    # Trimmed, deduplicated and sorted, as the Pages API returns them.
    assert customer.synonyms == ("Account", "Buyer")
    assert customer.state == "PUBLISHED"
    # A schema object without business name is titled by its name.
    assert pages[1].display_name == "order"


def test_the_body_of_a_table_page_documents_its_contract(make_project: MakeProject) -> None:
    pages = by_key(pages_of(project(make_project)))
    customer = pages["table:customer"].body
    order = pages["table:order"].body

    assert customer.startswith(
        "Table `prod.sales_orders.customer` of the data product *sales.orders*.\n\nCustomers of the shop.\n\n"
        "## Data product\n\nOrders of the shop.\n\n## Key\n\n`customer_id`\n\n## Columns\n\n"
        "| Column | Name | Type | Description |\n| --- | --- | --- | --- |\n"
    )
    # The pipes of the descriptions are escaped, and the code lists link to their Page.
    assert "| `customer_id` | Customer number | `STRING` | Identifier \\| of the customer. |" in customer
    assert "| `segment` | Segment | `STRING` | Segment of the customer. Codes: see the Page *Segment*. |" in customer
    assert customer.endswith("## References\n\n- [Glossary of the shop](https://example.com/glossary/customer)")
    # A column without business name has an empty name.
    assert "| `order_id` |  | `BIGINT` |  |" in order
    assert "## Relationships\n\n- `customer_id`: *Client* (`customer.customer_id`), many to one" in order
    assert (
        "| `channel` |  | `STRING` | Codes: the values of `prod.sales_orders.customer.segment` "
        "where `segment <> 'B2C'`. |" in order
    )
    assert order.endswith(
        "## Service levels\n\n- Frequency: Loaded every day.\n- At most 2 % of the rows fail the quality checks."
    )


def test_columns_with_the_same_code_list_share_its_page(make_project: MakeProject) -> None:
    pages = by_key(pages_of(project(make_project)))
    segment = pages["code_list:Ventes:segment"]
    status = pages["code_list:Ventes:order status"]

    assert segment.body == (
        "Codes of the columns `customer.segment` (*Client*), `order.customer_segment` (*order*).\n\n"
        "| Code | Label |\n| --- | --- |\n| `B2B` | Business |\n| `B2C` | Consumer |"
    )
    assert segment.description == "Segment of the customer."
    assert [table.name for table in segment.tables] == ["customer", "order"]
    # A value with a description adds the column.
    assert "| Code | Label | Description |" in status.body
    assert "| `open` | Open | Not shipped yet |\n| `closed` | Closed |  |" in status.body
    assert status.synonyms == ("State",)


def test_columns_of_the_same_title_need_the_same_codes(make_project: MakeProject) -> None:
    channel = "\n          - name: channel"
    order = ORDER.replace(f"value: B2C, label: Consumer }}{channel}", f"value: B2X }}{channel}")
    root = project(make_project, {"sales/orders/order/order.yaml": order})

    with pytest.raises(ContractError, match=r"order.customer_segment: its code list 'Segment' has other codes"):
        pages_of(root)


def test_pages_are_drafts_outside_the_discover_environments(make_project: MakeProject) -> None:
    pages = pages_of(project(make_project), environment="dev")

    assert {page.state for page in pages} == {"DRAFT"}
    assert pages[0].body.startswith("Table `dev.sales_orders.customer`")


def test_tables_without_discover_domain_have_no_page(make_project: MakeProject) -> None:
    group = GROUP.replace("discover_domain=True", "discover_domain=False")

    assert pages_of(project(make_project, {"sales/sales_config.py": group})) == []


def test_deprecated_contracts_have_no_page(make_project: MakeProject) -> None:
    order = ORDER.replace("version: 1.0.0\n", "version: 1.0.0\n    status: deprecated\n")
    pages = pages_of(project(make_project, {"sales/orders/order/order.yaml": order}))

    assert [page.key for page in pages] == ["table:customer", "code_list:Ventes:segment"]
    assert [table.name for table in pages[1].tables] == ["customer"]


def test_code_lists_can_stay_in_the_table_pages(make_project: MakeProject) -> None:
    product = PRODUCT.replace("PagesSettings(enabled=True)", "PagesSettings(enabled=True, code_lists=False)")
    pages = pages_of(project(make_project, {"sales/orders/orders_config.py": product}))

    assert [page.key for page in pages] == ["table:customer", "table:order"]
    assert "| `segment` | Segment | `STRING` | Segment of the customer. Values: B2B (Business), B2C (Consumer). |" in (
        pages[0].body
    )


@pytest.mark.parametrize(
    ("synonyms", "message"),
    [
        ("Buyer", "must be a list of strings"),
        ("[Buyer, 3]", "must be a list of strings"),
        ('["  "]', "a synonym must have from 1 to 200 characters"),
        (f"[{'x' * 201}]", "a synonym must have from 1 to 200 characters"),
        (f"[{', '.join(f's{index}' for index in range(51))}]", "more than 50 synonyms"),
    ],
)
def test_invalid_synonyms_are_rejected(make_project: MakeProject, synonyms: str, message: str) -> None:
    customer = CUSTOMER.replace('[" Buyer ", Account, Buyer]', synonyms)

    with pytest.raises(ContractError, match=rf"sales.orders.customer: .*{message}"):
        pages_of(project(make_project, {"sales/orders/customer/customer.yaml": customer}))


def test_two_pages_of_a_domain_cannot_have_the_same_title(make_project: MakeProject) -> None:
    customer = CUSTOMER.replace("businessName: Client", "businessName: segment")

    with pytest.raises(ContractError, match=r"have the same title 'Segment' in the Discover domain 'Ventes'"):
        pages_of(project(make_project, {"sales/orders/customer/customer.yaml": customer}))


def test_a_title_has_at_most_200_characters(make_project: MakeProject) -> None:
    customer = CUSTOMER.replace("businessName: Client", f"businessName: {'C' * 201}")

    with pytest.raises(ContractError, match=r"must have from 1 to 200 characters"):
        pages_of(project(make_project, {"sales/orders/customer/customer.yaml": customer}))


def test_authoritative_definitions_are_read() -> None:
    contract = DataContract.from_dict(
        {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "id": "c",
            "version": "1",
            "schema": [
                {
                    "name": "t",
                    "authoritativeDefinitions": [{"url": "https://a", "type": "businessDefinition"}],
                    "properties": [{"name": "p", "authoritativeDefinitions": [{"url": "https://b"}]}],
                }
            ],
        }
    )

    [table] = contract.schema
    assert table.authoritative_definitions == (AuthoritativeDefinition("https://a", "businessDefinition"),)
    assert table.properties[0].authoritative_definitions == (AuthoritativeDefinition("https://b"),)
    with pytest.raises(ContractError, match=r"schema\[0\].authoritativeDefinitions\[0\].url: is required"):
        DataContract.from_dict(
            {**contract.raw, "schema": [{"name": "t", "authoritativeDefinitions": [{"type": "businessDefinition"}]}]}
        )


def test_the_settings_are_checked() -> None:
    with pytest.raises(ConfigurationError, match=r"PagesSettings.code_lists must be a boolean"):
        PagesSettings(code_lists="yes")  # type: ignore[arg-type]


def test_validate_reports_the_pages(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    root = project(make_project)

    assert main(["validate", str(root)]) == 0
    assert "  + Pages: 2 table(s), 2 code list(s), in the Discover domain(s) Ventes, published in prod" in (
        capsys.readouterr().out
    )


def test_validate_rejects_the_same_title_in_two_data_products(
    make_project: MakeProject, capsys: pytest.CaptureFixture[str]
) -> None:
    other = {
        "sales/returns/returns_config.py": PRODUCT.replace('name="orders"', 'name="returns"'),
        "sales/returns/refund/refund.yaml": """
            apiVersion: v3.2.0
            kind: DataContract
            id: sales.returns.refund
            version: 1.0.0
            schema:
              - name: refund
                businessName: Client
                properties:
                  - name: refund_id
                    physicalType: BIGINT
        """,
    }
    root = project(make_project, other)

    assert main(["validate", str(root)]) == 1
    assert (
        "x Pages: the Pages of the table sales.orders.customer and of the table sales.returns.refund have the "
        "same title 'Client' in the Discover domain 'Ventes'"
    ) in capsys.readouterr().out


def test_the_pages_are_printed(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    root = project(make_project)

    assert main(["pages", str(root), "--data-product", "sales.orders", "--environment", "prod"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert [page["key"] for page in printed] == [page.key for page in pages_of(root)]
    assert printed[0]["synonyms"] == ["Account", "Buyer"]


def test_pages_must_be_enabled_to_be_printed(project: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["pages", str(project), "--data-product", "sales.orders"]) == 1
    assert "has no Pages: enable them with PagesSettings(enabled=True)" in capsys.readouterr().err


def test_the_project_of_the_other_tests_has_no_pages(make_project: MakeProject) -> None:
    assert pages_of(make_project(PROJECT)) == []


def test_the_markdown_of_the_contracts_is_escaped(make_project: MakeProject) -> None:
    customer = (
        CUSTOMER.replace("{ value: B2C, label: Consumer }", '{ value: "B|C", label: Consumer }')
        .replace("https://example.com/glossary/customer", "https://example.com/Client (v2)")
        .replace("description: Glossary of the shop", "description: Glossary [shop]")
    )
    order = FILES["sales/orders/order/order.yaml"].replace(
        "value: B2C, label: Consumer }", 'value: "B|C", label: Consumer }'
    )
    pages = by_key(
        pages_of(
            project(
                make_project, {"sales/orders/customer/customer.yaml": customer, "sales/orders/order/order.yaml": order}
            )
        )
    )

    assert "| `B\|C` | Consumer |" in pages["code_list:Ventes:segment"].body
    assert pages["table:customer"].body.endswith("- [Glossary \[shop\]](https://example.com/Client%20%28v2%29)")


def test_a_blank_business_name_is_no_business_name(make_project: MakeProject) -> None:
    customer = CUSTOMER.replace("businessName: Client", 'businessName: " "')

    assert pages_of(project(make_project, {"sales/orders/customer/customer.yaml": customer}))[0].display_name == (
        "customer"
    )


def test_the_key_of_a_code_list_does_not_depend_on_the_case_of_its_title(make_project: MakeProject) -> None:
    customer = CUSTOMER.replace("businessName: Segment", "businessName: SEGMENT")
    pages = pages_of(project(make_project, {"sales/orders/customer/customer.yaml": customer}))

    assert pages[3].key == "code_list:Ventes:segment"
    assert pages[3].display_name == "SEGMENT"


def test_validate_reports_data_products_without_domain(
    make_project: MakeProject, capsys: pytest.CaptureFixture[str]
) -> None:
    group = GROUP.replace("discover_domain=True", "discover_domain=False")

    assert main(["validate", str(project(make_project, {"sales/sales_config.py": group}))]) == 0
    assert "  + Pages: none, no table is in a Discover domain\n" in capsys.readouterr().out
