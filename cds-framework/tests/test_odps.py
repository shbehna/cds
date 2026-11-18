import textwrap
from pathlib import Path
from typing import Any

import pytest
import yaml

from cds import ContractError
from cds.odps import DataProductDocument, contract_fields

PRODUCT = """
    apiVersion: v1.1.0
    kind: DataProduct
    id: sales.orders
    name: Orders
    version: 1.0.0
    status: active
    type: sourceAligned
    domain: Sales
    tenant: shop
    description:
      purpose: Orders of the shop.
      usage: Analysis of the sales.
      limitations: Updated daily.
    tags: [demo]
    customProperties:
      - property: subdomain
        value: Online
    support:
      - channel: orders
        tool: email
        url: mailto:orders@example.com
    outputPorts:
      - name: order
        contractId: sales.orders.order
        version: 1.0.0
"""


def product(text: str = PRODUCT) -> dict[str, Any]:
    data = yaml.safe_load(textwrap.dedent(text))
    assert isinstance(data, dict)
    return data


def test_parses_a_document() -> None:
    document = DataProductDocument.from_dict(product(), source="orders.odps.yaml")

    assert (document.id, document.api_version, document.name, document.version) == (
        "sales.orders",
        "v1.1.0",
        "Orders",
        "1.0.0",
    )
    assert (document.status, document.type, document.domain, document.tenant) == (
        "active",
        "sourceAligned",
        "Sales",
        "shop",
    )
    assert document.purpose == "Orders of the shop."
    assert document.tags == ("demo",)
    assert document.custom_properties == {"subdomain": "Online"}
    assert [(channel.channel, channel.url) for channel in document.support] == [("orders", "mailto:orders@example.com")]
    assert document.raw["outputPorts"][0]["contractId"] == "sales.orders.order"
    assert document.source == "orders.odps.yaml"


def test_reads_a_file(tmp_path: Path) -> None:
    path = tmp_path / "orders.odps.yaml"
    path.write_text(textwrap.dedent(PRODUCT), encoding="utf-8")

    assert DataProductDocument.from_file(path).id == "sales.orders"


def test_contract_fields_are_those_contracts_also_have() -> None:
    fields = contract_fields(DataProductDocument.from_dict(product()))

    assert fields == {
        "dataProduct": "sales.orders",
        "domain": "Sales",
        "tenant": "shop",
        "status": "active",
        "customProperties": [{"property": "subdomain", "value": "Online"}],
        "support": [{"channel": "orders", "tool": "email", "url": "mailto:orders@example.com"}],
        "description": {"usage": "Analysis of the sales.", "limitations": "Updated daily."},
    }


def test_contract_fields_keep_the_fields_of_the_support_channels_that_odcs_has() -> None:
    channel = {"channel": "orders", "url": "mailto:orders@example.com", "tags": ["a"], "authoritativeDefinitions": []}
    document = DataProductDocument.from_dict({**product(), "support": [channel]})

    assert contract_fields(document)["support"] == [{"channel": "orders", "url": "mailto:orders@example.com"}]


def test_contract_fields_skip_missing_values_and_empty_lists() -> None:
    document = DataProductDocument.from_dict(
        {"apiVersion": "v1.1.0", "kind": "DataProduct", "id": "s.d", "support": [], "description": {"purpose": "p"}}
    )

    assert contract_fields(document) == {"dataProduct": "s.d"}


def test_status_is_required_by_odps_1_0() -> None:
    data = {"apiVersion": "v1.0.0", "kind": "DataProduct", "id": "s.d"}

    with pytest.raises(ContractError, match=r"status: is required by ODPS v1\.0\.0"):
        DataProductDocument.from_dict(data)
    assert DataProductDocument.from_dict({**data, "status": "draft"}).status == "draft"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"kind": "DataContract"}, "kind: expected 'DataProduct'"),
        ({"apiVersion": "v2.0.0"}, "unsupported version 'v2.0.0'"),
        ({"id": None}, "id: is required"),
        ({"description": "Orders."}, "description: must be a mapping"),
        ({"description": {"purpose": ["a"]}}, "description.purpose: must be a string"),
        ({"support": [{"url": "mailto:a@example.com"}]}, r"support\[0\].channel: is required"),
        ({"customProperties": [{"value": 1}]}, r"customProperties\[0\]: requires 'property'"),
    ],
)
def test_invalid_documents(changes: dict[str, Any], message: str) -> None:
    with pytest.raises(ContractError, match=message):
        DataProductDocument.from_dict({**product(), **changes}, source="orders.odps.yaml")


@pytest.mark.parametrize(("content", "message"), [("- a\n", "must be a mapping"), ("id: [\n", "Cannot read")])
def test_invalid_files(tmp_path: Path, content: str, message: str) -> None:
    path = tmp_path / "orders.odps.yaml"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(ContractError, match=message):
        DataProductDocument.from_file(path)
