from collections.abc import Sequence

import pytest
from conftest import CUSTOMER, MakeProject
from fakes import FakeDataFrame, FakeSpark, Row

from cds import (
    ContractError,
    DataContext,
    RuntimeConfiguration,
    ServiceContext,
    ServiceReport,
    Status,
    TransformationService,
)
from cds.services.transformation import conform_to_contract

CUSTOMER_CONFIG = """
from cds import ContractConfig


def transform(spark, runtime_configuration, table):
    assert table.name == "customer"
    return spark.table(runtime_configuration.require("source"))


config = ContractConfig(name="customer", transform=transform)
"""


@pytest.fixture
def transformation_context(make_project: MakeProject, spark: FakeSpark) -> ServiceContext:
    root = make_project(
        {
            "sales/orders/customer/customer.yaml": CUSTOMER,
            "sales/orders/customer/customer_config.py": CUSTOMER_CONFIG,
            "sales/orders/order/order.yaml": CUSTOMER.replace("name: customer\n", "name: order\n"),
        }
    )
    return ServiceContext(
        spark=spark.as_session(),
        environment="dev",
        data_context=DataContext.discover(root),
        runtime_configuration=RuntimeConfiguration({"environment": "dev", "source": "raw.customers"}),
    )


def run(context: ServiceContext, tables: Sequence[str] = ()) -> ServiceReport:
    return TransformationService(context).run(context.get_data_product("sales.orders"), tables=list(tables))


def test_writes_the_transformation_cast_to_the_contract(
    transformation_context: ServiceContext, spark: FakeSpark
) -> None:
    spark.add_table("dev.sales_orders.customer", ["customer_id", "name", "created_on"])
    spark.tables["raw.customers"] = FakeDataFrame(["CREATED_ON", "name", "customer_id"], spark=spark)
    spark.on(r"^DESCRIBE HISTORY", [Row(operationMetrics={"numOutputRows": "3"})])

    report = run(transformation_context)

    results = {result.table: result for result in report.results}
    assert results["sales.orders.customer"].status is Status.SUCCESS
    assert "wrote 3 row(s)" in results["sales.orders.customer"].message
    assert results["sales.orders.order"].status is Status.SKIPPED
    [insert] = spark.statements_matching("^INSERT OVERWRITE")
    view = insert.rsplit(" ", 1)[1]
    assert insert == f"INSERT OVERWRITE TABLE `dev`.`sales_orders`.`customer` BY NAME SELECT * FROM {view}"
    written = spark.views[view]
    assert written.operations[-1] == (
        "selectExpr",
        (
            "CAST(`customer_id` AS STRING) AS `customer_id`",
            "CAST(`name` AS STRING) AS `name`",
            "CAST(`CREATED_ON` AS DATE) AS `created_on`",
        ),
    )
    spark.catalog.dropTempView.assert_called_once_with(view)


def test_requires_the_table(transformation_context: ServiceContext) -> None:
    [result] = run(transformation_context, tables=["customer"]).results

    assert result.status is Status.ERROR
    assert "run the schema service first" in result.message


def test_rejects_results_not_matching_the_contract(transformation_context: ServiceContext) -> None:
    table = transformation_context.data_context.get_table("customer")

    with pytest.raises(ContractError, match="missing columns: created_on; columns not in the contract: extra"):
        conform_to_contract(FakeDataFrame(["customer_id", "name", "extra"]).as_dataframe(), table)


TWO_TABLES = CUSTOMER.replace(
    "    schema:\n",
    "    schema:\n"
    "      - name: region\n"
    "        properties:\n"
    "          - name: code\n"
    "            physicalType: STRING\n",
)


def test_a_mapping_configures_contracts_with_several_tables(make_project: MakeProject, spark: FakeSpark) -> None:
    two_tables = TWO_TABLES
    root = make_project(
        {
            "s/d/c/c.yaml": two_tables,
            "s/d/c/c_config.py": (
                "from cds import ContractConfig\n"
                "config = ContractConfig(name='c', transform={'region': lambda spark: spark.table('raw.regions')})\n"
            ),
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table("dev.s_d.region", ["code"])
    spark.tables["raw.regions"] = FakeDataFrame(["code"], spark=spark)

    report = TransformationService(context).run(context.get_data_product("s.d"))

    assert [result.status for result in report.results] == [Status.SUCCESS, Status.SKIPPED]


def test_a_function_cannot_configure_several_tables(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(
        {
            "s/d/c/c.yaml": TWO_TABLES,
            "s/d/c/c_config.py": "from cds import ContractConfig\nconfig = ContractConfig(name='c', transform=print)\n",
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))

    report = TransformationService(context).run(context.get_data_product("s.d"))

    assert all(result.status is Status.ERROR and "must be a mapping" in result.message for result in report.results)


def test_a_mapping_must_hold_functions(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(
        {
            "s/d/c/c.yaml": TWO_TABLES,
            "s/d/c/c_config.py": (
                "from cds import ContractConfig\n"
                "config = ContractConfig(name='c', transform={'region': 'raw.regions'})\n"
            ),
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))

    [region, customer] = TransformationService(context).run(context.get_data_product("s.d")).results

    assert region.status is Status.ERROR and "the transform of region must be a function" in region.message
    assert customer.status is Status.SKIPPED
