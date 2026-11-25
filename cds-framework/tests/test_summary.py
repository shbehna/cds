from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pytest
from conftest import CUSTOMER, MakeProject
from fakes import FakeSpark, Row
from test_quality_scd_snapshot import CUSTOMER_TABLE, FakeEngine, quality_service, run

from cds import ConfigurationError, ContractError, DataContext, RuntimeConfiguration, ServiceContext, Status
from cds.cli import main
from cds.odcs import QualityRule
from cds.services.checks import build_checks, make_check
from cds.services.summary import (
    COLUMNS,
    SummaryRow,
    Threshold,
    create_statement,
    failures_query,
    insert_statement,
    rule_thresholds,
    summary_rows,
    summary_table,
)

CHECKED_AT = datetime(2026, 11, 24, 12, 30, tzinfo=timezone.utc)
SUMMARY_TABLE = "`dev`.`sales_orders`.`_quality_summary`"


@pytest.fixture
def engine() -> FakeEngine:
    return FakeEngine()


@pytest.mark.parametrize(
    ("threshold", "passing", "failing"),
    [
        (Threshold("mustBe", 0), 0, 1),
        (Threshold("mustNotBe", 0), 1, 0),
        (Threshold("mustBeGreaterThan", 10), 11, 10),
        (Threshold("mustBeGreaterOrEqualTo", 10), 10, 9),
        (Threshold("mustBeLessThan", 5), 4.9, 5),
        (Threshold("mustBeLessOrEqualTo", 5), 5, 5.1),
        (Threshold("mustBeBetween", 1, 3), 3, 4),
        (Threshold("mustNotBeBetween", 1, 3), 0, 1),
    ],
)
def test_threshold_operators(threshold: Threshold, passing: float, failing: float) -> None:
    assert threshold.passes(passing)
    assert not threshold.passes(failing)


def test_threshold_actual_values() -> None:
    assert Threshold("mustBeLessThan", 5, unit="percent").actual(failed_rows=3, total_rows=200) == 1.5
    assert Threshold("mustBe", 0).actual(failed_rows=3, total_rows=200) == 3
    assert Threshold("mustBeGreaterThan", 10, metric="rowCount").actual(failed_rows=200, total_rows=200) == 200
    # No row fails in an empty table.
    assert Threshold("mustBeLessThan", 5, unit="percent").actual(failed_rows=0, total_rows=0) == 0


def rule(**fields: Any) -> QualityRule:
    unit = fields.pop("unit", None)
    return QualityRule(type="library", name="r", metric="nullValues", thresholds=fields, unit=unit)


def test_rule_thresholds() -> None:
    assert rule_thresholds(rule(mustBeLessThan=5, unit="percent"), "t") == [
        Threshold("mustBeLessThan", 5, unit="percent")
    ]
    assert rule_thresholds(rule(mustBeBetween=[1, 3], unit="%"), "t") == [
        Threshold("mustBeBetween", 1, 3, unit="percent")
    ]
    assert rule_thresholds(rule(mustBe=0, unit="Rows"), "t") == [Threshold("mustBe", 0)]
    assert rule_thresholds(rule(), "t") == []


@pytest.mark.parametrize(
    ("fields", "metric", "message"),
    [
        ({"mustBe": "0"}, "failedRows", "t: quality rule 'r': mustBe must be a number, got '0'"),
        ({"mustBe": True}, "failedRows", "mustBe must be a number"),
        ({"mustBeBetween": [1]}, "failedRows", r"mustBeBetween must be a pair of numbers \[low, high\]"),
        ({"mustBeBetween": [3, 1]}, "failedRows", "the low bound 3.0 is above the high bound 1.0"),
        ({"mustBeLessThan": 120, "unit": "percent"}, "failedRows", "a percentage is from 0 to 100, got 120"),
        ({"mustBe": 0, "unit": "lines"}, "failedRows", "the unit must be rows or percent, got 'lines'"),
        ({"mustBeGreaterThan": 10, "unit": "percent"}, "rowCount", "a rowCount threshold is a number of rows"),
    ],
)
def test_rule_thresholds_are_validated(fields: dict[str, Any], metric: str, message: str) -> None:
    with pytest.raises(ContractError, match=message):
        rule_thresholds(rule(**fields), "t", metric)


def rows(**overrides: Any) -> list[SummaryRow]:
    arguments: dict[str, Any] = {
        "checked_at": CHECKED_AT,
        "run_id": "42",
        "data_product": "sales.orders",
        "table_name": "customer",
        "checks": [
            make_check("customer_id_is_null", "error", "is_not_null", column="customer_id"),
            make_check("name_mostly_present", "warn", "is_not_null", column="name"),
            make_check("row_count", "error", "is_aggr_not_less_than", column="*", aggr_type="count", limit=100),
        ],
        "thresholds": {
            "name_mostly_present": [Threshold("mustBeLessThan", 5, unit="percent")],
            "row_count": [Threshold("mustBeGreaterOrEqualTo", 100, metric="rowCount")],
        },
        "error_rate": Threshold("mustBeLessOrEqualTo", 1, unit="percent", metric="errorRate"),
        "total_rows": 200,
        "error_rows": 4,
        "warning_rows": 12,
        "failures": {"customer_id_is_null": 4, "name_mostly_present": 12, "unknown_check": 1},
    }
    return summary_rows(**{**arguments, **overrides})


def test_summary_rows() -> None:
    summary = {(row.check_name, row.metric): row for row in rows()}

    assert list(summary) == [
        (None, "errorRate"),
        (None, "warningRate"),
        ("customer_id_is_null", "failedRows"),
        ("name_mostly_present", "failedRows"),
        ("row_count", "rowCount"),
        ("unknown_check", "failedRows"),
    ]
    error_rate = summary[None, "errorRate"]
    assert (error_rate.failed_rows, error_rate.actual, error_rate.unit) == (4, 2.0, "percent")
    assert (error_rate.operator, error_rate.threshold, error_rate.passed) == ("mustBeLessOrEqualTo", 1, False)
    assert (error_rate.checked_at, error_rate.run_id, error_rate.data_product) == (CHECKED_AT, "42", "sales.orders")
    assert error_rate.criticality is None and error_rate.columns is None
    warning_rate = summary[None, "warningRate"]
    assert (warning_rate.actual, warning_rate.unit, warning_rate.operator, warning_rate.passed) == (
        6.0,
        "percent",
        None,
        None,
    )
    # A check without threshold: its statistics only, in rows.
    required = summary["customer_id_is_null", "failedRows"]
    assert (required.criticality, required.columns, required.actual, required.unit) == (
        "error",
        ("customer_id",),
        4,
        "rows",
    )
    assert required.operator is None and required.passed is None
    tolerated = summary["name_mostly_present", "failedRows"]
    assert (tolerated.criticality, tolerated.actual, tolerated.unit, tolerated.passed) == (
        "warn",
        6.0,
        "percent",
        False,
    )
    row_count = summary["row_count", "rowCount"]
    assert (row_count.failed_rows, row_count.actual, row_count.columns, row_count.passed) == (0, 200, None, True)
    # A check DQX named, absent from the checks of the contract.
    unknown = summary["unknown_check", "failedRows"]
    assert (unknown.criticality, unknown.columns, unknown.failed_rows) == (None, None, 1)


def test_summary_rows_without_sla_and_with_several_thresholds() -> None:
    thresholds = {"name_mostly_present": [Threshold("mustBeGreaterThan", 1), Threshold("mustBeLessThan", 20)]}

    summary = rows(error_rate=None, thresholds=thresholds, failures={})

    assert (summary[0].metric, summary[0].operator, summary[0].passed) == ("errorRate", None, None)
    assert [(row.operator, row.passed) for row in summary if row.check_name == "name_mostly_present"] == [
        ("mustBeGreaterThan", False),
        ("mustBeLessThan", True),
    ]


def test_summary_statements() -> None:
    create = create_statement("dev.sales_orders._quality_summary")
    assert create.startswith(f"CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (\n  `checked_at` TIMESTAMP COMMENT")
    assert "`columns` ARRAY<STRING> COMMENT" in create and "\nUSING DELTA\nCOMMENT 'Quality summary" in create
    assert list(COLUMNS) == list(SummaryRow.__dataclass_fields__)

    insert = insert_statement("dev.sales_orders._quality_summary", rows()[:4])
    header, *values = insert.split("\n")
    assert header == f"INSERT INTO {SUMMARY_TABLE} ({', '.join(f'`{name}`' for name in COLUMNS)}) VALUES"
    assert values[0] == (
        "  (TIMESTAMP '2026-11-24 12:30:00+00:00', '42', 'sales.orders', 'customer', CAST(NULL AS STRING), "
        "CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'errorRate', 200, 4, 2.0D, 'percent', "
        "'mustBeLessOrEqualTo', 1.0D, CAST(NULL AS DOUBLE), FALSE),"
    )
    assert values[2].startswith("  (TIMESTAMP '2026-11-24 12:30:00+00:00', '42', 'sales.orders', 'customer', ")
    assert "'customer_id_is_null', 'error', array('customer_id'), 'failedRows', 200, 4, 4.0D, 'rows'," in values[2]


def test_failures_query() -> None:
    query = failures_query("dev.s.t", ["_errors", "_warnings"])

    assert query == (
        "SELECT name, count(*) AS failed_rows\n"
        "FROM (SELECT explode(array_distinct(array_union("
        "coalesce(transform(`_errors`, r -> r.name), CAST(array() AS ARRAY<STRING>)), "
        "coalesce(transform(`_warnings`, r -> r.name), CAST(array() AS ARRAY<STRING>))))) AS name "
        "FROM `dev`.`s`.`t`) AS results\n"
        "WHERE name IS NOT NULL\nGROUP BY name"
    )
    assert "array_union" not in (failures_query("dev.s.t", ["_errors"]) or "")
    assert failures_query("dev.s.t", []) is None


RULES = CUSTOMER.replace(
    "            description: Name of the customer.\n",
    "            description: Name of the customer.\n"
    "            quality:\n"
    "              - metric: nullValues\n"
    "                name: name_mostly_present\n"
    "                severity: warning\n"
    "                mustBeLessThan: 5\n"
    "                unit: percent\n",
).replace(
    "    schema:",
    "    slaProperties:\n      - {property: errorRate, value: 1, unit: percent}\n    schema:",
)


def rules_context(make_project: MakeProject, spark: FakeSpark, files: dict[str, str] | None = None) -> ServiceContext:
    root = make_project({"sales/orders/customer/customer.yaml": RULES, **(files or {})})
    return ServiceContext(
        spark=spark.as_session(),
        environment="dev",
        data_context=DataContext.discover(root),
        runtime_configuration=RuntimeConfiguration({"environment": "dev", "run_id": "1234"}),
    )


def test_quality_appends_the_summary(make_project: MakeProject, spark: FakeSpark, engine: FakeEngine) -> None:
    context = rules_context(make_project, spark)
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"count_if", [Row(total=200, errors=1, warnings=12)])
    spark.on(
        r"AS failed_rows",
        [Row(name="customer_id_is_null", failed_rows=1), Row(name="name_mostly_present", failed_rows=12)],
    )

    [result] = run(quality_service(context, engine), context).results

    assert result.status is Status.WARNING
    assert result.message == (
        "3 check(s) on 200 row(s): 1 row(s) with errors, 12 with warnings; 1 threshold(s) exceeded: name_mostly_present"
    )
    [(_, checks)] = engine.calls
    assert make_check("name_mostly_present", "warn", "is_not_null", column="name") in checks
    [failures] = spark.statements_matching(r"AS failed_rows")
    assert "FROM `dev`.`sales_orders`.`customer`" in failures
    assert spark.statements_matching(rf"^CREATE TABLE IF NOT EXISTS {re.escape(SUMMARY_TABLE)} \(")
    [insert] = spark.statements_matching(rf"^INSERT INTO {re.escape(SUMMARY_TABLE)} ")
    values = insert.split("\n")[1:]
    assert len(values) == 5
    assert "'1234', 'sales.orders', 'customer', CAST(NULL AS STRING)" in values[0]
    assert "'errorRate', 200, 1, 0.5D, 'percent', 'mustBeLessOrEqualTo', 1.0D, CAST(NULL AS DOUBLE), TRUE" in values[0]
    assert "'name_mostly_present', 'warn', array('name'), 'failedRows', 200, 12, 6.0D, 'percent', " in values[3]
    assert values[3].endswith("'mustBeLessThan', 5.0D, CAST(NULL AS DOUBLE), FALSE),")


def test_quality_creates_the_summary_once_per_run(
    make_project: MakeProject, spark: FakeSpark, engine: FakeEngine
) -> None:
    other = RULES.replace("id: sales.orders.customer", "id: sales.orders.other").replace(
        "name: customer\n", "name: other\n"
    )
    context = rules_context(make_project, spark, {"sales/orders/other/other.yaml": other})
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.add_table("dev.sales_orders.other", ["customer_id", "name", "created_on"])
    spark.on(r"count_if", [Row(total=10, errors=0, warnings=0)])

    report = run(quality_service(context, engine), context, tables=())

    assert [result.status for result in report.results] == [Status.SUCCESS, Status.SUCCESS]
    assert len(spark.statements_matching(r"^CREATE TABLE IF NOT EXISTS .*_quality_summary")) == 1
    assert len(spark.statements_matching(r"^INSERT INTO .*_quality_summary")) == 2


def test_quality_summary_can_be_disabled_or_renamed(
    make_project: MakeProject, spark: FakeSpark, engine: FakeEngine
) -> None:
    config = (
        "from cds import ContractConfig, QualitySettings\n"
        "config = ContractConfig(name='customer', quality=QualitySettings(summary={summary}, summary_table='_dq'))\n"
    )
    spark.add_table(CUSTOMER_TABLE, ["customer_id", "name", "created_on"])
    spark.on(r"count_if", [Row(total=10, errors=0, warnings=0)])

    renamed = rules_context(
        make_project, spark, {"sales/orders/customer/customer_config.py": config.format(summary=True)}
    )
    run(quality_service(renamed, engine), renamed)
    assert spark.statements_matching(r"^INSERT INTO `dev`.`sales_orders`.`_dq` ")

    spark.statements.clear()
    disabled = rules_context(
        make_project, spark, {"sales/orders/customer/customer_config.py": config.format(summary=False)}
    )
    [result] = run(quality_service(disabled, engine), disabled).results
    assert result.status is Status.SUCCESS
    assert not spark.statements_matching(r"_dq|AS failed_rows")


def test_the_summary_cannot_replace_a_table(make_project: MakeProject, spark: FakeSpark) -> None:
    config = (
        "from cds import DataProductConfig, QualitySettings\n"
        "config = DataProductConfig(name='orders', quality=QualitySettings(summary_table='customer'))\n"
    )
    context = rules_context(make_project, spark, {"sales/orders/orders_config.py": config})

    with pytest.raises(ConfigurationError, match=r"the quality summary table dev\.sales_orders\.customer is the table"):
        summary_table(context.data_context.get_table("customer"), context)


def test_check_names_with_thresholds_are_unique(make_project: MakeProject, spark: FakeSpark) -> None:
    contract = RULES.replace("name: name_mostly_present", "name: name_is_null").replace(
        "            logicalType: string\n", "            logicalType: string\n            required: true\n"
    )
    context = rules_context(make_project, spark, {"sales/orders/customer/customer.yaml": contract})

    with pytest.raises(ContractError, match=r"two checks are named 'name_is_null': name the quality rule"):
        build_checks(context.data_context.get_table("customer"), context)


def test_dqx_rules_with_thresholds_need_named_checks(make_project: MakeProject, spark: FakeSpark) -> None:
    contract = CUSTOMER.replace(
        "            description: Name of the customer.\n",
        "            description: Name of the customer.\n"
        "            quality:\n"
        "              - type: custom\n"
        "                engine: dqx\n"
        "                mustBeLessThan: 3\n"
        "                implementation:\n"
        "                  - {check: {function: is_not_null}}\n"
        "                  - {check: {function: is_not_empty}}\n",
    )
    context = rules_context(make_project, spark, {"sales/orders/customer/customer.yaml": contract})

    with pytest.raises(ContractError, match=r"the DQX checks of a rule with thresholds need a name"):
        build_checks(context.data_context.get_table("customer"), context)


def test_cli_validate_reports_invalid_thresholds(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    root = make_project(
        {"sales/orders/customer/customer.yaml": RULES.replace("mustBeLessThan: 5", "mustBeLessThan: 500")}
    )

    assert main(["validate", str(root)]) == 1
    assert "a percentage is from 0 to 100, got 500" in capsys.readouterr().out
