from __future__ import annotations

import sys
import textwrap
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from fakes import FakeSpark

from cds import DataContext, ServiceContext

CUSTOMER = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.customer
    version: 1.0.0
    schema:
      - name: customer
        description: Customers of the shop.
        properties:
          - name: customer_id
            physicalType: STRING
            required: true
            primaryKey: true
            description: Identifier of the customer.
          - name: name
            logicalType: string
            description: Name of the customer.
          - name: created_on
            physicalType: DATE
"""

ORDER = """
    apiVersion: v3.2.0
    kind: DataContract
    id: sales.orders.order
    version: 1.2.0
    schema:
      - name: order
        description: Lines of the orders.
        properties:
          - name: order_id
            physicalType: BIGINT
            primaryKey: true
            primaryKeyPosition: 1
          - name: line
            physicalType: INT
            primaryKey: true
            primaryKeyPosition: 2
          - name: customer_id
            physicalType: STRING
            required: true
            relationships:
              - to: customer.customer_id
          - name: status
            physicalType: STRING
            enum:
              - value: open
              - value: closed
          - name: amount
            physicalType: DECIMAL(10,2)
            logicalTypeOptions:
              minimum: 0
          - name: updated_at
            physicalType: TIMESTAMP
"""

PROJECT = {
    "sales/sales_config.py": """
        from cds import GroupConfig

        config = GroupConfig(name="sales", contract_defaults={"tenant": "shop", "tags": ["demo"]})
    """,
    "sales/orders/orders_config.py": """
        from cds import DataProductConfig, QualitySettings

        config = DataProductConfig(
            name="orders",
            description="Orders of the shop.",
            quality=QualitySettings(criticality={"foreign_key": "warn"}),
        )
    """,
    "sales/orders/helpers.py": """
        def select_order_columns(df):
            return df.selectExpr("id AS `order_id`")
    """,
    "sales/orders/customer/customer.yaml": CUSTOMER,
    "sales/orders/order/order.yaml": ORDER,
    "sales/orders/order/order_config.py": """
        from cds import ContractConfig

        from ..helpers import select_order_columns


        def transform(spark, runtime_configuration):
            return select_order_columns(spark.table(runtime_configuration.require("source")))


        config = ContractConfig(name="order", transform=transform)
    """,
}


def write_files(root: Path, files: Mapping[str, str]) -> Path:
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")
    return root


class MakeProject(Protocol):
    """Writes a project layout from relative paths and contents (``PROJECT`` by default), returns its root."""

    def __call__(self, files: Mapping[str, str] | None = None, name: str = "products") -> Path: ...


@pytest.fixture
def make_project(tmp_path: Path) -> MakeProject:
    def make(files: Mapping[str, str] | None = None, name: str = "products") -> Path:
        return write_files(tmp_path / name, PROJECT if files is None else files)

    return make


@pytest.fixture
def project(make_project: MakeProject) -> Path:
    return make_project()


@pytest.fixture
def spark() -> FakeSpark:
    return FakeSpark()


@pytest.fixture
def context(project: Path, spark: FakeSpark) -> ServiceContext:
    return ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(project))
