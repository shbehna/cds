from typing import Any

import pytest
from conftest import CUSTOMER, ORDER, MakeProject
from fakes import FakeDataFrame, FakeSpark, Row

from cds import ConfigurationError, ContractError, DataContext, ServiceContext
from cds.services import checks as checks_module
from cds.services.checks import CheckSet, build_checks, checked_tables, literal_values, make_check

RULES = """
    apiVersion: v3.2.0
    kind: DataContract
    id: s.d.measure
    version: 1.0.0
    schema:
      - name: measure
        properties:
          - name: id
            physicalType: STRING
            unique: true
            logicalTypeOptions:
              pattern: "^[A-Z]{3}$"
              minLength: 3
              maxLength: 3
          - name: value
            physicalType: DOUBLE
            logicalTypeOptions:
              minimum: 0
              exclusiveMaximum: 100
            quality:
              - metric: nullValues
                mustBe: 0
                severity: warning
              - metric: invalidValues
                arguments:
                  validValues: [1, 2]
                mustBe: 0
              - metric: nullValues
                mustBeLessThan: 5
                unit: percent
              - type: custom
                engine: dqx
                implementation:
                  criticality: warn
                  check:
                    function: is_not_less_than
                    arguments:
                      limit: 1
              - type: custom
                engine: soda
                implementation: "missing_count(value) = 0"
              - type: text
                description: Values are measured daily.
          - name: day
            physicalType: DATE
            logicalTypeOptions:
              minimum: "2020-01-01"
        quality:
          - metric: rowCount
            mustBeGreaterThan: 10
          - metric: duplicateValues
            mustBe: 0
            arguments:
              properties: [id, day]
          - type: sql
            query: SELECT count(*) FROM measure
            mustBe: 0
"""


def checks_of(context: ServiceContext, table_name: str) -> CheckSet:
    return build_checks(context.data_context.get_table(table_name), context)


def make_context(spark: FakeSpark, files: dict[str, str], make_project: MakeProject) -> ServiceContext:
    return ServiceContext(
        spark=spark.as_session(), environment="dev", data_context=DataContext.discover(make_project(files))
    )


def test_checks_inferred_from_a_contract(context: ServiceContext) -> None:
    check_set = checks_of(context, "order")

    assert check_set.unsupported == []
    assert check_set.checks == [
        make_check("order_id_is_null", "error", "is_not_null", column="order_id"),
        make_check("line_is_null", "error", "is_not_null", column="line"),
        make_check("customer_id_is_null", "error", "is_not_null", column="customer_id"),
        make_check(
            "customer_id_not_in_customer",
            "warn",
            "foreign_key",
            columns=["customer_id"],
            ref_columns=["customer_id"],
            ref_table="`dev`.`sales_orders`.`customer`",
        ),
        make_check("status_is_not_in_enum", "error", "is_in_list", column="status", allowed=["'open'", "'closed'"]),
        make_check("amount_is_out_of_range", "error", "is_not_less_than", column="amount", limit=0),
        make_check("primary_key_is_not_unique", "error", "is_unique", columns=["order_id", "line"]),
    ]


def test_a_key_column_may_be_nullable(make_project: MakeProject, spark: FakeSpark) -> None:
    contract = ORDER.replace("physicalType: INT\n", "physicalType: INT\n            required: false\n")
    context = make_context(
        spark, {"s/d/customer/customer.yaml": CUSTOMER, "s/d/order/order.yaml": contract}, make_project
    )

    checks = {check["name"]: check for check in checks_of(context, "order").checks}

    assert "order_id_is_null" in checks and "line_is_null" not in checks
    # Rows with the same key, nulls included, are duplicates.
    assert checks["primary_key_is_not_unique"]["check"]["arguments"] == {
        "columns": ["order_id", "line"],
        "nulls_distinct": False,
    }


def test_checks_from_quality_rules(make_project: MakeProject, spark: FakeSpark) -> None:
    context = make_context(spark, {"s/d/measure/measure.yaml": RULES}, make_project)

    check_set = checks_of(context, "measure")
    checks = {check.get("name"): check for check in check_set.checks}

    assert checks["id_is_not_unique"]["check"] == {"function": "is_unique", "arguments": {"columns": ["id"]}}
    assert checks["id_does_not_match_pattern"]["check"]["arguments"] == {"column": "id", "regex": "^[A-Z]{3}$"}
    assert (
        checks["id_has_invalid_length"]["check"]["arguments"]["expression"]
        == "`id` IS NULL OR (length(`id`) >= 3 AND length(`id`) <= 3)"
    )
    assert checks["value_is_out_of_range"]["check"]["arguments"]["expression"] == "`value` >= 0 AND `value` < 100"
    assert checks["value_is_null"]["criticality"] == "warn"
    assert checks["value_is_invalid"]["check"]["arguments"] == {"column": "value", "allowed": [1, 2]}
    assert checks["day_is_out_of_range"]["check"]["arguments"]["expression"] == "`day` >= '2020-01-01'"
    assert checks["row_count_minimum"]["check"] == {
        "function": "is_aggr_not_less_than",
        "arguments": {"column": "*", "aggr_type": "count", "limit": 11},
    }
    assert checks["id_day_is_not_unique"]["check"]["arguments"] == {"columns": ["id", "day"]}
    [custom] = [check for check in check_set.checks if check["check"]["function"] == "is_not_less_than"]
    assert custom == {
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"limit": 1, "column": "value"}},
    }
    assert len(check_set.unsupported) == 3
    assert any("nullValues" in note for note in check_set.unsupported)
    assert any("engine 'soda'" in note for note in check_set.unsupported)
    assert any("sql rule" in note for note in check_set.unsupported)


OPTIONS = """
    apiVersion: v3.2.0
    kind: DataContract
    id: s.d.item
    version: 1.0.0
    schema:
      - name: item
        properties:
          - name: email
            physicalType: STRING
            logicalTypeOptions:
              format: email
          - name: day
            logicalType: date
            physicalType: STRING
            logicalTypeOptions:
              format: yyyy-MM-dd
          - name: born
            logicalType: date
            logicalTypeOptions:
              format: yyyy-MM-dd
          - name: price
            physicalType: DECIMAL(10,2)
            logicalTypeOptions:
              multipleOf: 0.05
          - name: tags
            logicalType: array
            physicalType: ARRAY<STRING>
            logicalTypeOptions:
              minItems: 1
              maxItems: 3
              uniqueItems: true
          - name: attributes
            logicalType: object
            physicalType: MAP<STRING, STRING>
            logicalTypeOptions:
              minProperties: 1
              required: [color]
          - name: address
            logicalType: object
            physicalType: STRUCT<street:STRING,city:STRING>
            logicalTypeOptions:
              required: [city]
              maxProperties: 2
          - name: updated_at
            logicalType: timestamp
            logicalTypeOptions:
              timezone: true
          - name: code
            physicalType: STRING
            logicalTypeOptions:
              minLenght: 2
              format: hostname
"""


def test_checks_from_logical_type_options(make_project: MakeProject, spark: FakeSpark) -> None:
    context = make_context(spark, {"s/d/item/item.yaml": OPTIONS}, make_project)

    check_set = checks_of(context, "item")
    checks = {check["name"]: check for check in check_set.checks}

    def arguments(name: str) -> dict[str, Any]:
        arguments: dict[str, Any] = checks[name]["check"]["arguments"]
        return arguments

    assert checks["email_has_invalid_format"]["check"] == {
        "function": "is_valid_email",
        "arguments": {"column": "email"},
    }
    assert checks["email_has_invalid_format"]["criticality"] == "error"
    assert checks["day_has_invalid_format"]["check"] == {
        "function": "is_valid_date",
        "arguments": {"column": "day", "date_format": "yyyy-MM-dd"},
    }
    assert arguments("price_is_not_multiple")["expression"] == "`price` IS NULL OR (`price` % 0.05 = 0)"
    assert arguments("tags_has_invalid_item_count")["expression"] == (
        "`tags` IS NULL OR (size(`tags`) >= 1 AND size(`tags`) <= 3)"
    )
    assert arguments("tags_has_duplicate_items")["expression"] == (
        "`tags` IS NULL OR (size(array_distinct(`tags`)) = size(`tags`))"
    )
    assert arguments("attributes_has_invalid_key_count")["expression"] == (
        "`attributes` IS NULL OR (size(`attributes`) >= 1)"
    )
    assert arguments("attributes_misses_required_properties")["expression"] == (
        "`attributes` IS NULL OR (map_contains_key(`attributes`, 'color'))"
    )
    assert arguments("address_misses_required_properties")["expression"] == (
        "`address` IS NULL OR (`address`.`city` IS NOT NULL)"
    )
    assert check_set.unsupported == [
        "s.d.item.born: logicalTypeOptions format: 'yyyy-MM-dd' on a DATE column",
        "s.d.item.address: logicalTypeOptions maxProperties: 2",
        "s.d.item.updated_at: logicalTypeOptions timezone: True",
        "s.d.item.code: logicalTypeOptions format: 'hostname'",
        "s.d.item.code: logicalTypeOptions minLenght: 2",
    ]
    dqx = pytest.importorskip("databricks.labs.dqx.engine")
    assert not dqx.DQEngine.validate_checks(check_set.checks).has_errors


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ("multipleOf: 0", "multipleOf must be a positive number"),
        ("minItems: -1", "minItems must be a non-negative integer"),
        ("maxLength: '3'", "maxLength must be a non-negative integer"),
        ("required: city", "required must be a list of property names"),
    ],
)
def test_invalid_logical_type_options(make_project: MakeProject, spark: FakeSpark, options: str, message: str) -> None:
    contract = CUSTOMER.replace(
        "            description: Name of the customer.\n",
        f"            description: Name of the customer.\n            logicalTypeOptions:\n              {options}\n",
    )
    context = make_context(spark, {"s/d/customer/customer.yaml": contract}, make_project)

    with pytest.raises(ContractError, match=rf"customer\.name: logicalTypeOptions {message}"):
        checks_of(context, "customer")


def test_configured_checks(make_project: MakeProject, spark: FakeSpark) -> None:
    files = {
        "s/d/customer/customer.yaml": CUSTOMER,
        "s/d/customer/customer_config.py": (
            "from cds import ContractConfig\n\n"
            "def checks(spark, table):\n"
            "    allowed = [row.code for row in spark.table('ref.allowed_names').collect()]\n"
            "    return [{'criticality': 'warn', 'check': {'function': 'is_in_list', "
            "'arguments': {'column': 'name', 'allowed': allowed}}}]\n\n"
            "config = ContractConfig(name='customer', quality_checks=checks)\n"
        ),
    }
    spark.tables["ref.allowed_names"] = FakeDataFrame(rows=[Row(code="a")], spark=spark)
    context = make_context(spark, files, make_project)

    configured = checks_of(context, "customer").checks[-1]

    assert configured["check"]["arguments"] == {"column": "name", "allowed": ["a"]}


CODES = """
    apiVersion: v3.2.0
    kind: DataContract
    id: s.d.code
    version: 1.0.0
    schema:
      - name: code
        properties:
          - name: kind
            physicalType: STRING
          - name: value
            physicalType: STRING
"""

CODES_TABLE = "dev.s_d.code"
CODES_QUERY = r"^SELECT DISTINCT `value` AS value FROM `dev`\.`s_d`\.`code` WHERE `value` IS NOT NULL"


def allowed_values(declaration: str, columns: tuple[str, ...] = ("name",)) -> str:
    """The customer contract, whose columns declare the allowedValues custom property."""
    contract = CUSTOMER
    for column in columns:
        contract = contract.replace(
            f"          - name: {column}\n",
            f"          - name: {column}\n            customProperties:\n"
            f"              - property: allowedValues\n                value: {declaration}\n",
        )
    return contract


def test_allowed_values_are_read_at_run_time(make_project: MakeProject, spark: FakeSpark) -> None:
    contract = allowed_values("{reference: code.value, filter: \"kind = 'name'\"}", ("customer_id", "name"))
    files = {"s/d/customer/customer.yaml": contract, "s/d/code/code.yaml": CODES}
    spark.add_table(CODES_TABLE, ["kind", "value"])
    spark.on(CODES_QUERY, [Row(value="b"), Row(value="0100")])
    context = make_context(spark, files, make_project)

    checks = {check["name"]: check for check in checks_of(context, "customer").checks}

    # DQX reads bare strings as SQL: the values are passed as string literals.
    assert checks["name_is_not_allowed"] == make_check(
        "name_is_not_allowed", "error", "is_in_list", column="name", allowed=["'0100'", "'b'"]
    )
    assert checks["customer_id_is_not_allowed"]["check"]["arguments"]["allowed"] == ["'0100'", "'b'"]
    # The columns sharing a reference and a filter read it once.
    assert spark.statements_matching(CODES_QUERY) == [
        "SELECT DISTINCT `value` AS value FROM `dev`.`s_d`.`code` WHERE `value` IS NOT NULL AND (kind = 'name') "
        "LIMIT 10001"
    ]
    dqx = pytest.importorskip("databricks.labs.dqx.engine")
    assert not dqx.DQEngine.validate_checks(list(checks.values())).has_errors


def test_allowed_values_criticality(make_project: MakeProject, spark: FakeSpark) -> None:
    files = {
        "s/s_config.py": (
            "from cds import GroupConfig, QualitySettings\n"
            "config = GroupConfig(name='s', quality=QualitySettings(criticality={'allowed_values': 'warn'}))\n"
        ),
        "s/d/customer/customer.yaml": allowed_values("{reference: code.value}"),
        "s/d/code/code.yaml": CODES,
    }
    spark.add_table(CODES_TABLE, ["kind", "value"])
    spark.on(CODES_QUERY, [Row(value="a")])
    context = make_context(spark, files, make_project)

    [check] = [check for check in checks_of(context, "customer").checks if check["name"] == "name_is_not_allowed"]

    assert check["criticality"] == "warn"
    assert spark.statements_matching(CODES_QUERY) == [
        "SELECT DISTINCT `value` AS value FROM `dev`.`s_d`.`code` WHERE `value` IS NOT NULL LIMIT 10001"
    ]


def test_allowed_values_without_reading_them(make_project: MakeProject, spark: FakeSpark) -> None:
    files = {
        "s/d/customer/customer.yaml": allowed_values("{reference: code.value, filter: \"kind = 'name'\"}"),
        "s/d/code/code.yaml": CODES,
    }
    context = make_context(spark, files, make_project)

    check_set = build_checks(context.data_context.get_table("customer"), context, read_values=False)

    assert "name_is_not_allowed" not in {check["name"] for check in check_set.checks}
    assert check_set.run_time == ["s.d.customer.name: allowedValues of s.d.code.value where kind = 'name'"]
    assert spark.statements_matching(CODES_QUERY) == []


def test_a_table_with_allowed_values_is_checked(make_project: MakeProject, spark: FakeSpark) -> None:
    contract = CODES.replace(
        "          - name: kind\n            physicalType: STRING\n",
        "          - name: kind\n            physicalType: STRING\n            customProperties:\n"
        "              - {property: allowedValues, value: {reference: code.value}}\n",
    )
    context = make_context(spark, {"s/d/code/code.yaml": contract}, make_project)

    tables = checked_tables(context.get_data_product("s.d"), context)

    # Its only check is computed at run time, without reading the values now.
    assert [table.name for table in tables] == ["code"]
    assert spark.statements_matching("SELECT DISTINCT") == []


@pytest.mark.parametrize(
    ("exists", "values", "note"),
    [
        (False, [], "the table dev.s_d.code does not exist"),
        # An empty reference would reject every value.
        (True, [], "no value in s.d.code.value"),
        (True, ["a", "b", "c"], "more than 2 values in s.d.code.value, declare a foreignKey relationship instead"),
    ],
)
def test_allowed_values_that_cannot_be_checked(
    make_project: MakeProject,
    spark: FakeSpark,
    monkeypatch: pytest.MonkeyPatch,
    exists: bool,
    values: list[str],
    note: str,
) -> None:
    monkeypatch.setattr(checks_module, "MAX_ALLOWED_VALUES", 2)
    files = {"s/d/customer/customer.yaml": allowed_values("{reference: code.value}"), "s/d/code/code.yaml": CODES}
    if exists:
        spark.add_table(CODES_TABLE, ["kind", "value"])
    spark.on(CODES_QUERY, [Row(value=value) for value in values])
    context = make_context(spark, files, make_project)

    check_set = checks_of(context, "customer")

    assert "name_is_not_allowed" not in {check["name"] for check in check_set.checks}
    assert check_set.unsupported == [f"s.d.customer.name: allowedValues: {note}"]
    # A missing table is not read; otherwise at most one more value than allowed is.
    assert [statement.rsplit(" ", 2)[1:] for statement in spark.statements_matching(CODES_QUERY)] == (
        [["LIMIT", "3"]] if exists else []
    )


@pytest.mark.parametrize(
    ("declaration", "message"),
    [
        ("code.value", "must be a mapping with a 'reference'"),
        ("{filter: \"kind = 'name'\"}", "must be a mapping with a 'reference'"),
        ("{reference: code.value, where: x}", r"unknown key\(s\) where"),
        ("{reference: code.value, filter: ''}", "filter must be a non-empty SQL expression"),
        ("{reference: code.missing}", "has no property 'missing'"),
        ("{reference: code.value, filter: \"kind = 'a') OR (1 = 1\"}", "has unbalanced parentheses"),
        ("{reference: code.value, filter: \"(kind = 'a'\"}", "has unbalanced parentheses"),
        ("{reference: code.value, filter: \"kind = 'a' -- x\"}", "must not hold comments or ';'"),
        ("{reference: code.value, filter: \"kind = 'a' /* x */\"}", "must not hold comments or ';'"),
        ("{reference: code.value, filter: \"kind = 'a'; SELECT 1\"}", "must not hold comments or ';'"),
        ('{reference: code.value, filter: "kind = \'a"}', "has an unterminated quote"),
    ],
)
def test_invalid_allowed_values(make_project: MakeProject, spark: FakeSpark, declaration: str, message: str) -> None:
    files = {"s/d/customer/customer.yaml": allowed_values(declaration), "s/d/code/code.yaml": CODES}
    context = make_context(spark, files, make_project)

    with pytest.raises(ContractError, match=message):
        checks_of(context, "customer")


@pytest.mark.parametrize(
    "expression",
    [
        "kind = 'a'",
        r"kind IN ('a)', 'l''eau', 'x\'--') AND (`value` <> ';')",
        "upper(\"kind\") = 'A' OR `a--b` = 1",
    ],
)
def test_filters_may_hold_parentheses_and_comment_marks_in_literals(expression: str) -> None:
    assert checks_module._filter_problem(expression) is None


def test_literal_values() -> None:
    assert literal_values(["open", "1", "l'eau", 2, 1.5, True]) == ["'open'", "'1'", "'l\\'eau'", 2, 1.5, True]


@pytest.mark.parametrize(
    ("quality_checks", "message"),
    [
        ("'not a list'", "must be a list"),
        ("[{'criticality': 'warn'}]", "'check' entry"),
    ],
)
def test_invalid_configured_checks(
    make_project: MakeProject, spark: FakeSpark, quality_checks: str, message: str
) -> None:
    files = {
        "s/d/customer/customer.yaml": CUSTOMER,
        "s/d/customer/customer_config.py": (
            "from cds import ContractConfig\n"
            f"config = ContractConfig(name='customer', quality_checks={quality_checks})\n"
        ),
    }
    context = make_context(spark, files, make_project)

    with pytest.raises(ConfigurationError, match=message):
        checks_of(context, "customer")


def schema_relationship(source: str, target: str) -> str:
    return ORDER.replace(
        "        description: Lines of the orders.\n",
        f"        description: Lines of the orders.\n        relationships:\n          - from: {source}\n"
        f"            to: {target}\n",
    )


def test_schema_level_relationships(make_project: MakeProject, spark: FakeSpark) -> None:
    order = schema_relationship("[order.customer_id, name]", "[customer.customer_id, customer.name]")
    files = {
        "s/d/customer/customer.yaml": CUSTOMER,
        "s/d/order/order.yaml": order.replace("- name: status", "- name: name"),
    }
    context = make_context(spark, files, make_project)

    [composite] = [
        check for check in checks_of(context, "order").checks if check["name"] == "customer_id_name_not_in_customer"
    ]

    assert composite["check"]["arguments"] == {
        "columns": ["customer_id", "name"],
        "ref_columns": ["customer_id", "name"],
        "ref_table": "`dev`.`s_d`.`customer`",
    }


def test_relationships_must_reference_a_single_table(make_project: MakeProject, spark: FakeSpark) -> None:
    order = schema_relationship("[customer_id, line]", "[customer.customer_id, order.line]")
    files = {"s/d/customer/customer.yaml": CUSTOMER, "s/d/order/order.yaml": order}
    context = make_context(spark, files, make_project)

    with pytest.raises(ContractError, match="single table"):
        checks_of(context, "order")


def test_inferred_checks_are_valid_for_dqx(make_project: MakeProject, spark: FakeSpark) -> None:
    dqx = pytest.importorskip("databricks.labs.dqx.engine")
    files = {"s/d/customer/customer.yaml": CUSTOMER, "s/d/order/order.yaml": ORDER, "s/d/measure/measure.yaml": RULES}
    context = make_context(spark, files, make_project)

    for table in context.data_context.tables:
        checks = build_checks(table, context).checks
        status = dqx.DQEngine.validate_checks(checks)
        assert not status.has_errors, (table.name, status.errors)


def test_invalid_dqx_implementation(make_project: MakeProject, spark: FakeSpark) -> None:
    contract = CUSTOMER.replace(
        "description: Name of the customer.\n",
        "description: Name of the customer.\n            quality:\n              - type: custom\n"
        "                engine: dqx\n                implementation: [1, 2]\n",
    )
    context = make_context(spark, {"s/d/customer/customer.yaml": contract}, make_project)

    with pytest.raises(ContractError, match="must be a check"):
        checks_of(context, "customer")
