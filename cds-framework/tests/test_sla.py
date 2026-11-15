"""Tests of the service level agreements of the contracts (cds.sla)."""

from typing import Any

import pytest

from cds import ContractError, DataContext
from cds.odcs import DataContract, SlaProperty, SupportChannel
from cds.sla import (
    Duration,
    canonical_property,
    cron_problem,
    duration,
    email_addresses,
    percentage,
    quartz_cron,
    sla_problems,
    support_emails,
    table_sla,
    targets,
)


def document(sla: list[dict[str, Any]], support: list[dict[str, Any]] | None = None, tables: int = 2) -> DataContract:
    schema = [
        {"name": "orders", "properties": [{"name": "id"}, {"name": "updated_at", "logicalType": "timestamp"}]},
        {"name": "lines", "properties": [{"name": "id"}]},
    ]
    return DataContract.from_dict(
        {
            "apiVersion": "v3.2.0",
            "kind": "DataContract",
            "id": "c",
            "version": "1.0.0",
            "schema": schema[:tables],
            "slaProperties": sla,
            "support": support or [],
        }
    )


def test_parses_the_sla_properties_and_support_channels() -> None:
    parsed = document(
        [
            {
                "property": "latency",
                "value": 4,
                "unit": "d",
                "element": "orders",
                "scheduler": "cron",
                "schedule": "0 7 * * *",
            }
        ],
        [{"channel": "team", "tool": "email", "url": "mailto:team@example.com", "scope": "notifications"}],
    )

    [latency] = parsed.sla_properties
    assert (latency.property, latency.value, latency.unit, latency.element) == ("latency", 4, "d", "orders")
    assert (latency.scheduler, latency.schedule) == ("cron", "0 7 * * *")
    [channel] = parsed.support
    assert (channel.channel, channel.tool, channel.url, channel.scope) == (
        "team",
        "email",
        "mailto:team@example.com",
        "notifications",
    )
    with pytest.raises(ContractError, match=r"slaProperties\[0\]\.property: is required"):
        document([{"value": 1}])
    with pytest.raises(ContractError, match=r"support\[0\]\.channel: is required"):
        document([], [{"tool": "email"}])


def test_property_names_and_abbreviations() -> None:
    assert canonical_property("errorRate") == canonical_property("ERRORRATE") == canonical_property("er")
    assert canonical_property("ly") == "latency"
    assert canonical_property("freshness") is None


@pytest.mark.parametrize(
    ("value", "unit", "expected"),
    [
        (4, "d", Duration(4, "DAY")),
        ("2", "weeks", Duration(14, "DAY")),
        (10, "y", Duration(10, "YEAR")),
        (6, "h", Duration(6, "HOUR")),
    ],
)
def test_durations(value: object, unit: str, expected: Duration) -> None:
    assert duration(SlaProperty("latency", value, unit=unit)) == expected


def test_duration_errors_and_formats() -> None:
    assert Duration(10, "YEAR").sql() == "INTERVAL '10' YEAR"
    assert (str(Duration(1, "DAY")), str(Duration(16, "DAY"))) == ("1 day", "16 days")
    with pytest.raises(ContractError, match="positive integer"):
        duration(SlaProperty("latency", 1.5, unit="d"))
    with pytest.raises(ContractError, match="the unit must be a duration"):
        duration(SlaProperty("latency", 4, unit="percent"))


def test_percentages() -> None:
    assert percentage(SlaProperty("errorRate", 1, unit="percent")) == 1.0
    assert percentage(SlaProperty("errorRate", "0.5")) == 0.5
    with pytest.raises(ContractError, match="from 0 to 100"):
        percentage(SlaProperty("errorRate", 120, unit="%"))
    with pytest.raises(ContractError, match="percent or %"):
        percentage(SlaProperty("errorRate", 1, unit="d"))


@pytest.mark.parametrize(
    ("cron", "quartz"),
    [
        ("0 6 1,15 * *", "0 0 6 1,15 * ?"),
        ("30 7 * * *", "0 30 7 * * ?"),
        ("*/15 8-18 * * 1-5", "0 */15 8-18 ? * MON-FRI"),
        ("0 0 * jan,jul 0,7", "0 0 0 ? jan,jul SUN"),
        # Every day of the week: 0 and 7 are Sunday, and * within a list is every day.
        ("0 6 * * 0-7", "0 0 6 * * ?"),
        ("0 6 * * 1,*", "0 0 6 * * ?"),
        ("0 6 * * */2", "0 0 6 ? * SUN,TUE,THU,SAT"),
        ("0 6 * * 5-7", "0 0 6 ? * SUN,FRI,SAT"),
        # Without *, a day matching either field matches: every day.
        ("0 6 1-31 * 1", "0 0 6 * * ?"),
    ],
)
def test_quartz_cron(cron: str, quartz: str) -> None:
    assert cron_problem(cron) is None
    assert quartz_cron(cron) == quartz


@pytest.mark.parametrize(
    ("cron", "problem"),
    [
        ("0 6 * *", "must have 5 fields"),
        ("60 6 * * *", "minute '60': '60' is not a value from 0 to 59"),
        ("0 6 15-1 * *", "invalid range"),
        ("0 6 */0 * *", "invalid step"),
        ("0 6 * DEC-FOO *", "'FOO' is not a value"),
        # Quartz refuses a step larger than the field.
        ("*/60 * * * *", "the step 60 is larger than 59"),
        # Only ASCII digits: int() cannot read the others, such as the superscript two.
        (f"*/{chr(0xB2)} * * * *", "invalid step"),
        (f"{chr(0x665)} 6 * * *", "is not a value from 0 to 59"),
    ],
)
def test_cron_problems(cron: str, problem: str) -> None:
    found = cron_problem(cron)
    assert found is not None and problem in found


def test_quartz_cannot_restrict_both_days() -> None:
    for cron in ("0 6 1 * 1", "0 6 */2 * 1"):
        with pytest.raises(ContractError, match="both the day of month and the day of week"):
            quartz_cron(cron)


def test_targets() -> None:
    contract = document([])
    orders, lines = contract.schema

    assert targets(SlaProperty("latency", 4), contract) == [(orders, None), (lines, None)]
    assert targets(SlaProperty("latency", 4, element="orders.updated_at, lines"), contract) == [
        (orders, orders.properties[1]),
        (lines, None),
    ]
    # In a contract with a single table, a column may be named alone.
    single = document([], tables=1)
    assert targets(SlaProperty("latency", 4, element="updated_at"), single) == [
        (single.schema[0], single.schema[0].properties[1])
    ]
    with pytest.raises(ContractError, match="designates no table"):
        targets(SlaProperty("latency", 4, element="customers"), contract)
    with pytest.raises(ContractError, match="designates no column of orders"):
        targets(SlaProperty("latency", 4, element="orders.created_at"), contract)
    for element in ("updated_at,", ", lines", " "):
        with pytest.raises(ContractError, match="has an empty part"):
            targets(SlaProperty("latency", 4, element=element), single if element != ", lines" else contract)


def test_the_default_element_of_odcs_3_0() -> None:
    """slaDefaultElement, deprecated since ODCS v3.1, applies to the properties without element."""
    contract = DataContract.from_dict({**document([]).raw, "slaDefaultElement": "orders.updated_at"})
    orders = contract.schema[0]

    assert targets(SlaProperty("latency", 4), contract) == [(orders, orders.properties[1])]
    assert targets(SlaProperty("latency", 4, element="lines"), contract) == [(contract.schema[1], None)]


def test_table_sla(make_project: Any) -> None:
    contract = """
        apiVersion: v3.2.0
        kind: DataContract
        id: c
        version: 1.0.0
        schema:
          - {name: orders, properties: [{name: id}, {name: updated_at, logicalType: timestamp}]}
          - {name: lines, properties: [{name: id}]}
        slaProperties:
          - {property: latency, value: 4, unit: d}
          - {property: ly, value: 1, unit: d, element: orders.updated_at}
    """
    data_context = DataContext.discover(make_project({"s/d/c/c.yaml": contract}))
    orders, lines = data_context.get_table("orders"), data_context.get_table("lines")

    # The property of the table takes precedence over the one without element.
    specific = table_sla(orders, "latency")
    assert specific is not None and (specific[0].value, specific[1]) == (1, "updated_at")
    general = table_sla(lines, "latency")
    assert general is not None and (general[0].value, general[1]) == (4, None)
    assert table_sla(lines, "retention") is None


def test_support_emails() -> None:
    contract = document(
        [],
        [
            {"channel": "team", "tool": "email", "url": "mailto:a@example.com,b%40example.com?subject=REQ"},
            {"channel": "again", "tool": "Email", "url": "a@example.com"},
            {"channel": "chat", "tool": "slack", "url": "https://example.slack.com/archives/c"},
        ],
    )

    assert support_emails(contract) == ["a@example.com", "b@example.com"]
    assert email_addresses(SupportChannel("web", tool="email", url="https://example.com")) == []


def test_sla_problems() -> None:
    contract = document(
        [
            {"property": "latency", "value": 16, "unit": "d", "scheduler": "cron", "schedule": "0 7 * * *"},
            {"property": "freshness", "value": 1},
            {"property": "retention", "value": -1, "unit": "y"},
            {"property": "errorRate", "value": 1, "unit": "percent", "scheduler": "cron", "schedule": "0 7 1 * 1"},
            {"property": "frequency", "value": 1, "unit": "d", "scheduler": "airflow", "schedule": "daily"},
            {"property": "latency", "value": 1, "unit": "d", "element": "orders.created_at"},
            # Other schedulers are allowed for the properties CDS does not schedule.
            {"property": "timeToRepair", "value": 4, "unit": "h", "scheduler": "pagerduty"},
            # A second latency of every table, and a second one of lines.
            {"property": "ly", "value": 2, "unit": "d"},
            {"property": "latency", "value": 2, "unit": "d", "element": "lines"},
            {"property": "latency", "value": 3, "unit": "d", "element": "lines"},
            # The retention applies to the history versions, not to a column.
            {"property": "retention", "value": 3, "unit": "y", "element": "orders.updated_at"},
        ],
        [{"channel": "team", "tool": "email"}],
    )

    problems = sla_problems(contract)

    assert len(problems) == 10
    assert problems[0].startswith("slaProperties[1]: unknown property 'freshness', expected one of availability")
    assert problems[1] == "slaProperties[2]: retention: the value must be a positive integer, got -1"
    assert "slaProperties[3]: the cron expression '0 7 1 * 1' restricts both" in problems[2]
    assert problems[3] == "slaProperties[4]: frequency: CDS reads cron schedules, got the scheduler 'airflow'"
    assert "slaProperties[5]: latency: the element 'orders.created_at' designates no column" in problems[4]
    assert problems[5:8] == [
        "slaProperties[7]: ly: the table orders already has this property (slaProperties[0])",
        "slaProperties[7]: ly: the table lines already has this property (slaProperties[0])",
        "slaProperties[9]: latency: the table lines already has this property (slaProperties[8])",
    ]
    assert problems[8] == (
        "slaProperties[10]: retention: the retention applies to the versions of the history: "
        "its element must designate tables"
    )
    assert problems[9] == "support[0]: the email channel 'team' needs a mailto: url"
