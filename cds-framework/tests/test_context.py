from pathlib import Path

import pytest
from conftest import CUSTOMER, MakeProject
from fakes import FakeSpark

from cds import ConfigurationError, DataContext, Environment, ServiceContext


def test_default_naming(context: ServiceContext) -> None:
    table = context.data_context.get_table("order")

    assert context.catalog_name(table) == "dev"
    assert context.schema_name(table.data_product) == "sales_orders"
    assert context.full_table_name(table) == "dev.sales_orders.order"
    assert context.full_table_name(table, "order_history") == "dev.sales_orders.order_history"


def test_naming_patterns_from_configuration(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(
        {
            "g/g_config.py": (
                "from cds import GroupConfig, NamingSettings\n"
                "config = GroupConfig(name='g', naming=NamingSettings(catalog='{environment}_{group}', "
                "schema='{data_product}'))\n"
            ),
            "g/d/c/c.yaml": CUSTOMER,
            "g/d/c/c_config.py": (
                "from cds import ContractConfig, NamingSettings\n"
                "config = ContractConfig(name='c', naming=NamingSettings(schema='{data_product}_{contract}'))\n"
            ),
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="prod", data_context=DataContext.discover(root))
    table = context.data_context.get_table("customer")

    assert context.full_table_name(table) == "prod_g.d_c.customer"
    assert context.full_schema_name(table.data_product) == "prod_g.d"


def test_invalid_naming_pattern(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(
        {
            "g/g_config.py": (
                "from cds import GroupConfig, NamingSettings\n"
                "config = GroupConfig(name='g', naming=NamingSettings(schema='{unknown}'))\n"
            ),
            "g/d/c/c.yaml": CUSTOMER,
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="prod", data_context=DataContext.discover(root))

    with pytest.raises(ConfigurationError, match="Available placeholders"):
        context.schema_name(context.data_context.get_table("customer"))


def test_create_reads_parameters_then_widgets_then_environment_variables(
    project: Path, spark: FakeSpark, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CDS_ENVIRONMENT", "nprod")
    monkeypatch.setenv("CDS_ROOTS", str(project))
    monkeypatch.setenv("CDS_DATA_PRODUCT", "orders")
    monkeypatch.setenv("CDS_GROUP", "sales")

    context = ServiceContext.create(spark=spark.as_session(), parameters={"max_workers": 2})

    assert context.environment == Environment("nprod")
    assert context.runtime_configuration.get_int("max_workers") == 2
    assert context.get_data_product().qualified_name == "sales.orders"
    assert context.spark is spark.as_session()


def test_create_requires_roots_and_environment(
    project: Path, spark: FakeSpark, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CDS_ROOTS", raising=False)
    monkeypatch.delenv("CDS_ENVIRONMENT", raising=False)

    with pytest.raises(ConfigurationError, match="No data product root"):
        ServiceContext.create(spark=spark.as_session())
    with pytest.raises(ConfigurationError, match="'environment' is required"):
        ServiceContext.create(spark=spark.as_session(), roots=project)


def test_invalid_environment() -> None:
    with pytest.raises(ConfigurationError, match="Invalid environment id"):
        Environment("Prod-1")


def test_data_product_is_required(context: ServiceContext) -> None:
    with pytest.raises(ConfigurationError, match="No data product to process"):
        context.get_data_product()


def test_offline_context_has_no_spark(project: Path) -> None:
    context = ServiceContext(spark=None, environment="dev", data_context=DataContext.discover(project))

    assert not context.has_spark
    with pytest.raises(ConfigurationError, match="no Spark session"):
        _ = context.spark
