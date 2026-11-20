from collections.abc import Sequence

import pytest
from conftest import CUSTOMER, MakeProject
from fakes import FakeField, FakeSpark, Row

from cds import DataContext, MetadataService, SchemaService, Service, ServiceContext, ServiceReport, Status

CUSTOMER_TABLE = "dev.sales_orders.customer"


def run(service_class: type[Service], context: ServiceContext, tables: Sequence[str] = ("customer",)) -> ServiceReport:
    service = service_class(context)
    return service.run(context.data_context.get_data_product("sales.orders"), tables=list(tables))


def test_creates_the_schema_and_missing_tables(context: ServiceContext, spark: FakeSpark) -> None:
    report = run(SchemaService, context, tables=())

    assert [result.status for result in report.results] == [Status.SUCCESS, Status.SUCCESS]
    assert spark.statements[0] == ("CREATE SCHEMA IF NOT EXISTS `dev`.`sales_orders` COMMENT 'Orders of the shop.'")
    [customer] = spark.statements_matching(r"^CREATE TABLE IF NOT EXISTS `dev`.`sales_orders`.`customer`")
    assert "`customer_id` STRING COMMENT 'Identifier of the customer.'" in customer
    assert "COMMENT 'Customers of the shop.'" in customer
    assert "'cds.contract.version' = '1.0.0'" in customer
    [order] = spark.statements_matching(r"^CREATE TABLE IF NOT EXISTS `dev`.`sales_orders`.`order`")
    assert "`amount` DECIMAL(10,2)" in order


def test_leaves_a_matching_table_untouched(context: ServiceContext, spark: FakeSpark) -> None:
    spark.add_table(
        CUSTOMER_TABLE,
        [
            FakeField("created_on", "date"),
            FakeField("customer_id", "string"),
            FakeField("name", "string"),
            FakeField("_errors", "array<struct<name:string>>"),
        ],
    )

    [result] = run(SchemaService, context).results

    assert result.status is Status.SUCCESS and "matches its contract" in result.message
    assert not spark.statements_matching("CREATE TABLE|ALTER TABLE")


def test_adds_the_columns_added_to_the_contract(context: ServiceContext, spark: FakeSpark) -> None:
    spark.add_table(CUSTOMER_TABLE, [FakeField("customer_id", "string"), FakeField("name", "string")])

    [result] = run(SchemaService, context).results

    assert result.status is Status.SUCCESS
    assert spark.statements_matching(r"ALTER TABLE `dev`.`sales_orders`.`customer` ADD COLUMNS \(`created_on` DATE\)")


def test_replaces_a_table_with_breaking_changes(context: ServiceContext, spark: FakeSpark) -> None:
    spark.add_table(
        CUSTOMER_TABLE,
        [FakeField("customer_id", "int"), FakeField("name", "string"), FakeField("created_on", "date"), "legacy"],
    )

    [result] = run(SchemaService, context).results

    assert result.status is Status.WARNING
    assert "customer_id (int -> string)" in result.message and "legacy" in result.message
    assert spark.statements_matching(r"^CREATE OR REPLACE TABLE `dev`.`sales_orders`.`customer`")


def test_breaking_changes_can_be_errors(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(
        {
            "sales/orders/orders_config.py": (
                "from cds import DataProductConfig, SchemaSettings\n"
                "config = DataProductConfig(name='orders', schema=SchemaSettings(on_breaking_change='error'))\n"
            ),
            "sales/orders/customer/customer.yaml": CUSTOMER,
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id"])
    spark.tables[CUSTOMER_TABLE] = spark.tables[CUSTOMER_TABLE].with_fields(FakeField("name", "int"))

    [result] = run(SchemaService, context).results

    assert result.status is Status.ERROR
    assert not spark.statements_matching("CREATE OR REPLACE")


def test_metadata_documents_existing_tables(context: ServiceContext, spark: FakeSpark) -> None:
    spark.add_table(
        CUSTOMER_TABLE,
        [FakeField("customer_id", "string", comment="Identifier of the customer."), FakeField("Name", "string")],
    )

    report = run(MetadataService, context, tables=())

    statuses = {result.table: result.status for result in report.results}
    assert statuses == {"sales.orders.customer": Status.SUCCESS, "sales.orders.order": Status.SKIPPED}
    assert spark.statements_matching(r"^COMMENT ON TABLE `dev`.`sales_orders`.`customer` IS 'Customers of the shop.'")
    assert spark.statements_matching(r"ALTER COLUMN `Name` COMMENT 'Name of the customer.'")
    assert not spark.statements_matching(r"ALTER COLUMN `customer_id`")
    assert spark.statements_matching(r"SET TBLPROPERTIES \('cds.contract.id' = 'sales.orders.customer'")


TAGGED_PROJECT = {
    "sales/sales_config.py": """
        from cds import GroupConfig, MetadataSettings

        config = GroupConfig(
            name="sales",
            metadata=MetadataSettings(certification={"active": "certified"}),
            contract_defaults={"status": "active", "tags": ["demo"]},
        )
    """,
    "sales/orders/customer/customer.yaml": CUSTOMER.replace(
        "description: Name of the customer.",
        "description: Name of the customer.\n            classification: restricted",
    ),
}


def test_metadata_synchronizes_the_tags(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(TAGGED_PROJECT)
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, [FakeField("customer_id", "string"), FakeField("Name", "string"), "created_on"])
    spark.on(
        r"FROM `dev`.`information_schema`.`table_tags` WHERE schema_name = 'sales_orders'",
        [
            Row(table_name="customer", tag_name="demo", tag_value=""),
            Row(table_name="customer", tag_name="owner", tag_value="ops"),
            Row(table_name="customer", tag_name="legacy", tag_value="x"),
        ],
    )
    spark.on(
        r"FROM `dev`.`information_schema`.`column_tags`",
        [
            Row(table_name="customer", column_name="name", tag_name="class.name", tag_value=None),
            Row(table_name="customer", column_name="customer_id", tag_name="pii", tag_value="id"),
        ],
    )
    spark.on(
        r"^SHOW TBLPROPERTIES `dev`.`sales_orders`.`customer`$",
        [Row(key="cds.tags", value='{"table":["demo","legacy"],"columns":{"customer_id":["pii"]}}')],
    )

    [result] = run(MetadataService, context).results

    assert result.status is Status.SUCCESS and "4 tag(s) set or removed" in result.message
    table = "ALTER TABLE `dev`.`sales_orders`.`customer`"
    tag_statements = spark.statements_matching(r"SET TAGS|UNSET TAGS")
    assert tag_statements == [
        f"{table} SET TAGS ('system.certification_status' = 'certified')",
        f"{table} UNSET TAGS ('legacy')",
        f"{table} ALTER COLUMN `customer_id` UNSET TAGS ('pii')",
        f"{table} ALTER COLUMN `Name` SET TAGS ('classification' = 'restricted')",
    ]
    [properties] = spark.statements_matching(r"SET TBLPROPERTIES")
    recorded = '{"columns":{"Name":["classification"]},"table":["demo","system.certification_status"]}'
    assert f"'cds.tags' = '{recorded}'" in properties
    assert spark.statements.index(properties) == len(spark.statements) - 1


@pytest.mark.parametrize(("environment", "tagged"), [("prod", True), ("dev", False)])
def test_metadata_tags_the_discover_domain_in_its_environments(
    make_project: MakeProject, spark: FakeSpark, environment: str, tagged: bool
) -> None:
    group = """
        from cds import GroupConfig, MetadataSettings

        config = GroupConfig(
            name="sales",
            metadata=MetadataSettings(discover_domain=True, discover_environments=["prod"]),
            contract_defaults={"domain": "Ventes"},
        )
    """
    root = make_project({**TAGGED_PROJECT, "sales/sales_config.py": group})
    context = ServiceContext(spark=spark.as_session(), environment=environment, data_context=DataContext.discover(root))
    spark.add_table(
        f"{environment}.sales_orders.customer", [FakeField("customer_id", "string"), FakeField("Name", "string")]
    )

    [result] = run(MetadataService, context).results

    assert result.status is Status.SUCCESS
    assert bool(spark.statements_matching(r"SET TAGS \(.*'Ventes' = ''")) is tagged


def test_metadata_tags_the_columns_of_the_history_table(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(TAGGED_PROJECT)
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.add_table(f"{CUSTOMER_TABLE}_history", ["customer_id", "Name", "created_on", "_valid_from", "_is_current"])
    spark.on(
        r"^SHOW TBLPROPERTIES `dev`.`sales_orders`.`customer_history`$",
        [Row(key="cds.tags", value='{"table":[],"columns":{"created_on":["pii"]}}')],
    )
    spark.on(
        r"FROM `dev`.`information_schema`.`column_tags`",
        [Row(table_name="customer_history", column_name="created_on", tag_name="pii", tag_value="")],
    )

    [result] = run(MetadataService, context).results

    assert result.status is Status.SUCCESS and "history table included" in result.message
    history = "ALTER TABLE `dev`.`sales_orders`.`customer_history`"
    assert spark.statements_matching(r"customer_history.*TAGS") == [
        f"{history} ALTER COLUMN `created_on` UNSET TAGS ('pii')",
        f"{history} ALTER COLUMN `Name` SET TAGS ('classification' = 'restricted')",
    ]
    # Only the tags of the columns: the history is not certified.
    assert not spark.statements_matching(r"customer_history` SET TAGS")
    [properties] = spark.statements_matching(r"customer_history` SET TBLPROPERTIES")
    assert """'cds.tags' = '{"columns":{"Name":["classification"]},"table":[]}'""" in properties


def test_metadata_leaves_the_tags_when_disabled(make_project: MakeProject, spark: FakeSpark) -> None:
    files = dict(TAGGED_PROJECT)
    files["sales/sales_config.py"] = """
        from cds import GroupConfig, MetadataSettings

        config = GroupConfig(name="sales", metadata=MetadataSettings(tags=False), contract_defaults={"tags": ["demo"]})
    """
    root = make_project(files)
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])

    [result] = run(MetadataService, context).results

    assert result.status is Status.SUCCESS and "tag" not in result.message
    assert not spark.statements_matching(r"information_schema|TAGS|SHOW TBLPROPERTIES|cds\.tags")


def tagged_context(make_project: MakeProject, spark: FakeSpark) -> ServiceContext:
    root = make_project(TAGGED_PROJECT)
    return ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))


def test_schema_tags_the_tables_it_creates(make_project: MakeProject, spark: FakeSpark) -> None:
    context = tagged_context(make_project, spark)

    [result] = run(SchemaService, context).results

    assert result.status is Status.SUCCESS and result.message.endswith("(3 columns), 1 tag(s) set")
    [create] = spark.statements_matching(r"^CREATE TABLE")
    # The columns only: the tags of the table, such as its certification, are set by the metadata service.
    assert spark.statements[-2:] == [
        create,
        "ALTER TABLE `dev`.`sales_orders`.`customer` ALTER COLUMN `name` SET TAGS ('classification' = 'restricted')",
    ]
    assert """'cds.tags' = '{"columns":{"name":["classification"]},"table":[]}'""" in create


def test_schema_drops_a_new_table_it_cannot_tag(make_project: MakeProject, spark: FakeSpark) -> None:
    context = tagged_context(make_project, spark)
    spark.on(r"ALTER COLUMN `name` SET TAGS", RuntimeError("PERMISSION_DENIED"))

    [result] = run(SchemaService, context).results

    assert result.status is Status.ERROR and "PERMISSION_DENIED" in result.message
    assert spark.statements[-1] == "DROP TABLE IF EXISTS `dev`.`sales_orders`.`customer`"


def test_schema_tags_the_columns_it_adds(make_project: MakeProject, spark: FakeSpark) -> None:
    context = tagged_context(make_project, spark)
    spark.add_table(CUSTOMER_TABLE, [FakeField("customer_id", "string"), FakeField("created_on", "date")])

    [result] = run(SchemaService, context).results

    assert result.status is Status.SUCCESS and result.message.endswith(": name, 1 tag(s) set")
    assert spark.statements_matching(r"TAGS") == [
        "ALTER TABLE `dev`.`sales_orders`.`customer` ALTER COLUMN `name` SET TAGS ('classification' = 'restricted')"
    ]


def test_schema_tags_the_tables_it_replaces(make_project: MakeProject, spark: FakeSpark) -> None:
    context = tagged_context(make_project, spark)
    spark.add_table(CUSTOMER_TABLE, [FakeField("customer_id", "int"), "name", FakeField("created_on", "date")])

    [result] = run(SchemaService, context).results

    assert result.status is Status.WARNING and result.message.endswith(", 1 tag(s) set")
    assert len(spark.statements_matching(r"SET TAGS")) == 1
    assert not spark.statements_matching(r"^DROP")


def test_schema_leaves_the_tags_when_disabled(make_project: MakeProject, spark: FakeSpark) -> None:
    files = dict(TAGGED_PROJECT)
    files["sales/sales_config.py"] = """
        from cds import GroupConfig, MetadataSettings

        config = GroupConfig(name="sales", metadata=MetadataSettings(tags=False), contract_defaults={"tags": ["demo"]})
    """
    root = make_project(files)
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))

    [result] = run(SchemaService, context).results

    assert result.status is Status.SUCCESS and "tag" not in result.message
    assert not spark.statements_matching(r"TAGS|cds\.tags")


@pytest.mark.parametrize("service_class", [SchemaService, MetadataService])
def test_unknown_tables_are_rejected(context: ServiceContext, service_class: type[Service]) -> None:
    from cds import ConfigurationError

    with pytest.raises(ConfigurationError, match="Unknown table"):
        run(service_class, context, tables=["supplier"])
