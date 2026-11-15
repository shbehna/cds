from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest
from conftest import CUSTOMER, MakeProject
from fakes import FakeDataFrame, FakeField, FakeSpark, FakeType, Row

from cds import (
    DataContext,
    QualityService,
    ScdService,
    Service,
    ServiceContext,
    ServiceReport,
    SnapshotService,
    Status,
)
from cds.services.scd import merge_statement

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

RESULT_TYPE = FakeType(
    "array<struct<name:string,message:string>>",
    {
        "type": "array",
        "elementType": {
            "type": "struct",
            "fields": [{"name": "name", "type": "string"}, {"name": "message", "type": "string"}],
        },
    },
)
CUSTOMER_TABLE = "dev.sales_orders.customer"


class FakeEngine:
    def __init__(self) -> None:
        self.calls: list[tuple[FakeDataFrame, list[dict[str, Any]]]] = []

    def apply_checks_by_metadata(self, df: DataFrame, checks: list[dict[str, Any]]) -> DataFrame:
        fake = cast(FakeDataFrame, df)
        self.calls.append((fake, checks))
        return fake.with_fields(FakeField("_errors", RESULT_TYPE), FakeField("_warnings", RESULT_TYPE)).as_dataframe()


def run(service: Service, context: ServiceContext, tables: Sequence[str] = ("customer",)) -> ServiceReport:
    return service.run(context.data_context.get_data_product("sales.orders"), tables=list(tables))


@pytest.fixture
def engine() -> FakeEngine:
    return FakeEngine()


def quality_service(context: ServiceContext, engine: FakeEngine, problems: Sequence[str] = ()) -> QualityService:
    return QualityService(context, engine_factory=lambda settings: engine, validator=lambda checks: list(problems))


def test_quality_annotates_the_rows(context: ServiceContext, spark: FakeSpark, engine: FakeEngine) -> None:
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on", FakeField("_errors", RESULT_TYPE)])
    spark.on(r"count_if", [Row(total=10, errors=2, warnings=0)])

    [result] = run(quality_service(context, engine), context).results

    assert result.status is Status.WARNING
    assert result.message == "2 check(s) on 10 row(s): 2 row(s) with errors, 0 with warnings"
    [(checked, checks)] = engine.calls
    assert checked.columns == ["customer_id", "name", "created_on"]
    assert [check["name"] for check in checks] == ["customer_id_is_null", "primary_key_is_not_unique"]
    assert spark.statements_matching(
        r"ALTER TABLE `dev`.`sales_orders`.`customer` ADD COLUMNS \(`_warnings` ARRAY<STRUCT<`name`: STRING, "
        r"`message`: STRING>>\)"
    )
    assert spark.statements_matching(r"^INSERT OVERWRITE TABLE `dev`.`sales_orders`.`customer` BY NAME")


def test_quality_without_errors_succeeds(context: ServiceContext, spark: FakeSpark, engine: FakeEngine) -> None:
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"count_if", [Row(total=10, errors=0, warnings=3)])

    [result] = run(quality_service(context, engine), context).results

    assert result.status is Status.SUCCESS
    assert "3 with warnings" in result.message


def test_quality_rejects_invalid_checks(context: ServiceContext, spark: FakeSpark, engine: FakeEngine) -> None:
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])

    [result] = run(quality_service(context, engine, problems=["bad check"]), context).results

    assert result.status is Status.ERROR and "bad check" in result.message
    assert engine.calls == []


def test_quality_warns_of_options_not_checked(
    make_project: MakeProject,
    spark: FakeSpark,
    engine: FakeEngine,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # run_service stops the propagation of the logs of the framework, which caplog captures at the root.
    monkeypatch.setattr(logging.getLogger("cds"), "propagate", True)
    contract = CUSTOMER.replace(
        "            description: Name of the customer.\n",
        "            description: Name of the customer.\n"
        "            logicalTypeOptions:\n"
        "              format: hostname\n",
    )
    root = make_project({"sales/orders/customer/customer.yaml": contract})
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"count_if", [Row(total=10, errors=0, warnings=0)])

    with caplog.at_level(logging.WARNING, logger="cds"):
        [result] = run(quality_service(context, engine), context).results

    assert result.status is Status.SUCCESS
    assert "not checked: sales.orders.customer.name: logicalTypeOptions format: 'hostname'" in result.message
    assert [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING] == [
        "quality: not checked, no check implements sales.orders.customer.name: logicalTypeOptions format: 'hostname'"
    ]


def test_quality_skips_missing_and_disabled_tables(
    make_project: MakeProject, spark: FakeSpark, engine: FakeEngine
) -> None:
    root = make_project(
        {
            "sales/orders/customer/customer.yaml": CUSTOMER,
            "sales/orders/other/other.yaml": CUSTOMER.replace("name: customer\n", "name: other\n"),
            "sales/orders/other/other_config.py": (
                "from cds import ContractConfig, QualitySettings\n"
                "config = ContractConfig(name='other', quality=QualitySettings(enabled=False))\n"
            ),
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))

    report = run(quality_service(context, engine), context, tables=())

    assert [(result.status, result.message) for result in report.results] == [
        (Status.SKIPPED, "dev.sales_orders.customer does not exist"),
        (Status.SKIPPED, "quality checks disabled"),
    ]


def scd_service(context: ServiceContext) -> ScdService:
    return ScdService(context, timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))


def test_scd_creates_the_history_and_merges(context: ServiceContext, spark: FakeSpark) -> None:
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on", "_errors"])
    spark.on(r"duplicate_keys", [Row(duplicate_keys=0)])
    spark.on(r"^DESCRIBE HISTORY", [Row(operationMetrics={"numTargetRowsInserted": "5", "numTargetRowsUpdated": "1"})])

    [result] = run(scd_service(context), context).results

    assert result.status is Status.SUCCESS
    assert result.message == "5 version(s) inserted and 1 closed in dev.sales_orders.customer_history"
    [create] = spark.statements_matching(r"^CREATE TABLE IF NOT EXISTS `dev`.`sales_orders`.`customer_history`")
    assert (
        "`_row_hash` STRING,\n  `_valid_from` TIMESTAMP,\n  `_valid_to` TIMESTAMP,\n  `_is_current` BOOLEAN" in create
    )
    [merge] = spark.statements_matching(r"^MERGE INTO")
    assert "`_errors`" not in merge
    assert "ON t.`customer_id` <=> s.`customer_id` AND t.`_is_current` AND s.`__cds_match`" in merge
    assert "timestamp_micros(1767225600000000)" in merge


HISTORY_TABLE = "ALTER TABLE `dev`.`sales_orders`.`customer_history`"
CLASSIFIED_CUSTOMER = CUSTOMER.replace(
    "description: Name of the customer.", "description: Name of the customer.\n            classification: restricted"
)


def test_scd_tags_the_columns_of_the_history_it_creates(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project({"sales/orders/customer/customer.yaml": CLASSIFIED_CUSTOMER})
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"duplicate_keys", [Row(duplicate_keys=0)])

    [result] = run(scd_service(context), context).results

    assert result.status is Status.SUCCESS
    [create] = spark.statements_matching(r"^CREATE TABLE IF NOT EXISTS `dev`.`sales_orders`.`customer_history`")
    assert """'cds.tags' = '{"columns":{"name":["classification"]},"table":[]}'""" in create
    tagging = f"{HISTORY_TABLE} ALTER COLUMN `name` SET TAGS ('classification' = 'restricted')"
    # The columns are tagged before the first version is written.
    assert spark.statements_matching(r"customer_history.*TAGS") == [tagging]
    assert spark.statements.index(tagging) < spark.statements.index(spark.statements_matching(r"^MERGE")[0])


def test_scd_drops_a_new_history_it_cannot_tag(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project({"sales/orders/customer/customer.yaml": CLASSIFIED_CUSTOMER})
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"SET TAGS", RuntimeError("PERMISSION_DENIED"))

    [result] = run(scd_service(context), context).results

    assert result.status is Status.ERROR and "PERMISSION_DENIED" in result.message
    assert spark.statements[-1] == "DROP TABLE IF EXISTS `dev`.`sales_orders`.`customer_history`"
    assert not spark.statements_matching(r"^MERGE")


def test_scd_tags_the_columns_it_adds_to_the_history(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project({"sales/orders/customer/customer.yaml": CLASSIFIED_CUSTOMER})
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.add_table(
        "dev.sales_orders.customer_history",
        ["customer_id", "created_on", "_row_hash", "_valid_from", "_valid_to", "_is_current"],
    )
    spark.on(r"duplicate_keys", [Row(duplicate_keys=0)])

    run(scd_service(context), context)

    assert spark.statements_matching(r"ADD COLUMNS|TAGS") == [
        f"{HISTORY_TABLE} ADD COLUMNS (`name` STRING)",
        f"{HISTORY_TABLE} ALTER COLUMN `name` SET TAGS ('classification' = 'restricted')",
    ]


def test_scd_fails_on_duplicate_keys_by_default(context: ServiceContext, spark: FakeSpark) -> None:
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"duplicate_keys", [Row(duplicate_keys=3)])

    [result] = run(scd_service(context), context).results

    assert result.status is Status.ERROR
    assert "3 key(s) have several rows" in result.message
    assert not spark.statements_matching("^MERGE")


def test_scd_can_deduplicate(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(
        {
            "sales/orders/orders_config.py": (
                "from cds import DataProductConfig, ScdSettings\n"
                "config = DataProductConfig(name='orders', scd=ScdSettings(on_duplicate_keys='deduplicate'))\n"
            ),
            "sales/orders/customer/customer.yaml": CUSTOMER,
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.add_table("dev.sales_orders.customer_history", ["customer_id", "name", "_row_hash", "_valid_from"])
    spark.on(r"duplicate_keys", [Row(duplicate_keys=3)])

    [result] = run(scd_service(context), context).results

    assert result.status is Status.WARNING
    assert "kept one row for each of 3 duplicate key(s)" in result.message
    assert spark.statements_matching(
        r"ALTER TABLE `dev`.`sales_orders`.`customer_history` ADD COLUMNS \(`created_on` DATE, "
        r"`_valid_to` TIMESTAMP, `_is_current` BOOLEAN\)"
    )
    [merge] = spark.statements_matching(r"^MERGE INTO")
    assert merge.count("QUALIFY row_number() OVER (PARTITION BY `customer_id` ORDER BY `_row_hash`) = 1") == 2


def test_scd_purges_the_versions_older_than_the_retention(make_project: MakeProject, spark: FakeSpark) -> None:
    contract = CUSTOMER + "    slaProperties:\n      - {property: retention, value: 10, unit: y}\n"
    root = make_project({"sales/orders/customer/customer.yaml": contract})
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"duplicate_keys", [Row(duplicate_keys=0)])
    spark.on(r"^DELETE FROM", [Row(num_affected_rows=3)])

    [result] = run(scd_service(context), context).results

    assert result.status is Status.SUCCESS
    assert result.message.endswith(", 3 version(s) closed for more than 10 years purged")
    # The current versions are kept; the closed ones are compared with the time of the run.
    [delete] = spark.statements_matching(r"^DELETE FROM")
    assert delete == (
        "DELETE FROM `dev`.`sales_orders`.`customer_history` WHERE NOT `_is_current` AND "
        "`_valid_to` < timestamp_micros(1767225600000000) - INTERVAL '10' YEAR"
    )
    assert spark.statements.index(delete) > spark.statements.index(spark.statements_matching(r"^MERGE INTO")[0])


def test_scd_keeps_every_version_without_retention(context: ServiceContext, spark: FakeSpark) -> None:
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"duplicate_keys", [Row(duplicate_keys=0)])

    [result] = run(scd_service(context), context).results

    assert result.status is Status.SUCCESS and "purged" not in result.message
    assert not spark.statements_matching(r"^DELETE")


def test_scd_skips_tables_without_primary_key(make_project: MakeProject, spark: FakeSpark) -> None:
    no_key = CUSTOMER.replace("            primaryKey: true\n", "")
    context = ServiceContext(
        spark=spark.as_session(),
        environment="dev",
        data_context=DataContext.discover(make_project({"sales/orders/customer/customer.yaml": no_key})),
    )
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])

    [result] = run(scd_service(context), context).results

    assert (result.status, result.message) == (Status.SKIPPED, "no primary key in the contract")


def test_merge_statement(context: ServiceContext) -> None:
    table = context.data_context.get_table("order")
    from cds.config import ScdSettings

    statement = merge_statement(
        source="c.s.order",
        target="c.s.order_history",
        keys=table.primary_key,
        columns=table.columns,
        settings=table.settings(ScdSettings),
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    hash_expression = (
        "sha2(to_json(named_struct('customer_id', `customer_id`, 'status', `status`, 'amount', `amount`, "
        "'updated_at', unix_micros(`updated_at`))), 256)"
    )
    rows = (
        "SELECT `order_id`, `line`, `customer_id`, `status`, `amount`, `updated_at`, "
        f"{hash_expression} AS `_row_hash` FROM `c`.`s`.`order`"
    )
    at = "timestamp_micros(1767225600000000)"
    assert (
        statement
        == f"""MERGE INTO `c`.`s`.`order_history` AS t
USING (
  SELECT s.*, FALSE AS `__cds_match` FROM ({rows}) AS s
  INNER JOIN `c`.`s`.`order_history` AS h ON s.`order_id` <=> h.`order_id` AND s.`line` <=> h.`line` AND h.`_is_current`
  WHERE s.`_row_hash` <> h.`_row_hash`
  UNION ALL
  SELECT s.*, TRUE AS `__cds_match` FROM ({rows}) AS s
) AS s
ON t.`order_id` <=> s.`order_id` AND t.`line` <=> s.`line` AND t.`_is_current` AND s.`__cds_match`
WHEN MATCHED AND t.`_row_hash` <> s.`_row_hash` THEN
  UPDATE SET t.`_valid_to` = {at}, t.`_is_current` = FALSE
WHEN NOT MATCHED THEN
  INSERT (`order_id`, `line`, `customer_id`, `status`, `amount`, `updated_at`, `_row_hash`, `_valid_from`, `_valid_to`, `_is_current`)
  VALUES (s.`order_id`, s.`line`, s.`customer_id`, s.`status`, s.`amount`, s.`updated_at`, s.`_row_hash`, {at}, NULL, TRUE)
WHEN NOT MATCHED BY SOURCE AND t.`_is_current` THEN
  UPDATE SET t.`_valid_to` = {at}, t.`_is_current` = FALSE"""  # noqa: E501
    )


def test_snapshot_loads_the_latest_records(make_project: MakeProject, spark: FakeSpark) -> None:
    root = make_project(
        {
            "sales/orders/orders_config.py": (
                "from cds import DataProductConfig, SnapshotSettings\n"
                "config = DataProductConfig(name='orders', snapshot=SnapshotSettings(enabled=True))\n"
            ),
            "sales/orders/customer/customer.yaml": CUSTOMER,
            "sales/orders/movement/movement.yaml": CUSTOMER.replace("name: customer\n", "name: movement\n"),
            "sales/orders/movement/movement_config.py": (
                "from cds import ContractConfig, SnapshotSettings\n"
                "config = ContractConfig(name='movement', snapshot=SnapshotSettings(source='raw.ledger.movements', "
                "order_by=['moved_at DESC'], condition='kind <> \"test\"'))\n"
            ),
        }
    )
    context = ServiceContext(spark=spark.as_session(), environment="dev", data_context=DataContext.discover(root))
    spark.add_table(CUSTOMER_TABLE, ["customer_id"])
    spark.add_table("dev.sales_orders.movement", ["customer_id"])

    report = run(SnapshotService(context), context, tables=())

    assert [result.status for result in report.results] == [Status.SUCCESS, Status.SUCCESS]
    assert spark.statements_matching(r"^INSERT OVERWRITE TABLE `dev`.`sales_orders`.`customer` BY NAME")[0] == (
        "INSERT OVERWRITE TABLE `dev`.`sales_orders`.`customer` BY NAME SELECT `customer_id`, `name`, `created_on` "
        "FROM `dev`.`sales_orders`.`customer_history` QUALIFY row_number() OVER (PARTITION BY `customer_id` "
        "ORDER BY `_valid_from` DESC) = 1"
    )
    assert spark.statements_matching(r"^INSERT OVERWRITE TABLE `dev`.`sales_orders`.`movement`")[0].endswith(
        'FROM `raw`.`ledger`.`movements` WHERE kind <> "test" QUALIFY row_number() OVER (PARTITION BY '
        "`customer_id` ORDER BY moved_at DESC) = 1"
    )


def test_snapshot_is_disabled_by_default(context: ServiceContext) -> None:
    [result] = run(SnapshotService(context), context).results

    assert (result.status, result.message) == (Status.SKIPPED, "snapshot not enabled")
