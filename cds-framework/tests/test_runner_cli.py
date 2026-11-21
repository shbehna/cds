import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from conftest import PROJECT, MakeProject
from fakes import FakeSpark

from cds import ConfigurationError, SchemaService, ServiceError, Status, run_service
from cds.cli import main


def test_run_service(project: Path, spark: FakeSpark) -> None:
    dbutils = MagicMock()
    dbutils.widgets.getAll.return_value = {"environment": "", "data_product": "sales.orders"}

    report = run_service(
        SchemaService, spark=spark.as_session(), dbutils=dbutils, roots=project, environment="nprod", max_workers=1
    )

    assert [result.status for result in report.results] == [Status.SUCCESS, Status.SUCCESS]
    assert spark.statements[0].startswith("CREATE SCHEMA IF NOT EXISTS `nprod`.`sales_orders`")
    declared = {call.args[0] for call in dbutils.widgets.text.call_args_list}
    assert {"environment", "roots", "data_product", "tables", "max_workers"} <= declared


def test_run_service_raises_when_a_table_fails(project: Path) -> None:
    spark = FakeSpark()
    spark.on(r"^CREATE TABLE IF NOT EXISTS `dev`.`sales_orders`.`order`", RuntimeError("permission denied"))

    with pytest.raises(ServiceError, match=r"sales\.orders\.order: RuntimeError: permission denied"):
        run_service(
            SchemaService, spark=spark.as_session(), roots=[project], environment="dev", data_product="sales.orders"
        )


def test_run_service_requires_a_service_class(project: Path, spark: FakeSpark) -> None:
    with pytest.raises(ConfigurationError, match=r"expects a service class, e\.g\. SchemaService, got 'schema'"):
        run_service(
            "schema",  # type: ignore[arg-type]
            spark=spark.as_session(),
            roots=project,
            environment="dev",
            data_product="sales.orders",
        )


def test_cli_validate(project: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate", str(project)]) == 0

    output = capsys.readouterr().out
    assert "sales.orders: 2 contract(s)" in output
    assert "+ order: 6 column(s), key (order_id, line), 7 check(s)" in output


ALLOWED_VALUES = """
    apiVersion: v3.2.0
    kind: DataContract
    id: c
    version: 1.0.0
    schema:
      - name: code
        properties:
          - {name: value, physicalType: STRING}
      - name: t
        properties:
          - name: a
            physicalType: STRING
            customProperties:
              - {property: allowedValues, value: {reference: code.value}}
"""


def test_cli_validate_reports_the_checks_computed_at_run_time(
    make_project: MakeProject, capsys: pytest.CaptureFixture[str]
) -> None:
    root = make_project({"s/d/c/c.yaml": ALLOWED_VALUES})

    assert main(["validate", str(root)]) == 0
    output = capsys.readouterr().out
    assert "+ t: 1 column(s), key (none), 0 check(s) + checks computed at run time" in output
    assert "computed at run time: s.d.t.a: allowedValues of s.d.code.value" in output


def test_cli_checks_lists_the_checks_computed_at_run_time(
    make_project: MakeProject, capsys: pytest.CaptureFixture[str]
) -> None:
    root = make_project({"s/d/c/c.yaml": ALLOWED_VALUES})

    assert main(["checks", str(root), "--data-product", "s.d"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["t"]["run_time_checks"] is True
    assert result["t"]["computed_at_run_time"] == ["s.d.t.a: allowedValues of s.d.code.value"]


TAGS_CONFIG = """
    from cds import GroupConfig, MetadataSettings

    config = GroupConfig(name="s", metadata=MetadataSettings(classifications=["public", "restricted"]))
"""

TAGS_CONTRACT = """
    apiVersion: v3.2.0
    kind: DataContract
    id: c
    version: 1.0.0
    schema:
      - name: t
        properties:
          - {name: a, physicalType: STRING, classification: secret}
          - {name: b, physicalType: STRING, classification: public}
"""


def test_cli_validate_reports_invalid_tags(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    contract = TAGS_CONTRACT.replace("schema:", "tags: [open-data]\n    schema:")
    root = make_project({"s/d/c/c.yaml": contract})

    assert main(["validate", str(root)]) == 1
    assert "x t: invalid tags: s.d.t: invalid tag 'open-data'" in capsys.readouterr().out


def test_cli_validate_reports_invalid_classifications(
    make_project: MakeProject, capsys: pytest.CaptureFixture[str]
) -> None:
    root = make_project({"s/s_config.py": TAGS_CONFIG, "s/d/c/c.yaml": TAGS_CONTRACT})

    assert main(["validate", str(root)]) == 1
    output = capsys.readouterr().out
    assert "x t: invalid tags: the classification 'secret' of a is not one of public, restricted" in output
    assert "classification 'public'" not in output


def test_cli_validate_reports_invalid_sla(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    contract = TAGS_CONTRACT.replace(
        "schema:",
        "slaProperties:\n"
        "      - {property: latency, value: 4, unit: d, element: t.b}\n"
        "      - {property: errorRate, value: 1, unit: percent, scheduler: cron, schedule: '0 7 * *'}\n"
        "    schema:",
    )
    root = make_project({"s/d/c/c.yaml": contract})

    assert main(["validate", str(root)]) == 1
    output = capsys.readouterr().out
    assert "+ t: 2 column(s)" in output
    assert "x contract c: invalid SLA: slaProperties[1]: the cron expression '0 7 * *' must have 5 fields" in output
    assert "slaProperties[0]" not in output


def test_cli_validate_reports_errors(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    root = make_project({"s/d/c/c.yaml": "apiVersion: v3.2.0\nkind: DataContract\nversion: 1\n"})

    assert main(["validate", str(root)]) == 1
    assert "id: is required" in capsys.readouterr().err


def test_cli_checks(project: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["checks", str(project), "--data-product", "sales.orders", "--environment", "prod"]) == 0

    assert '"ref_table": "`prod`.`sales_orders`.`customer`"' in capsys.readouterr().out


def test_cli_validate_shows_the_environments_of_the_domains(
    make_project: MakeProject, capsys: pytest.CaptureFixture[str]
) -> None:
    group = """
        from cds import GroupConfig, MetadataSettings

        config = GroupConfig(
            name="sales",
            contract_defaults={"domain": "Ventes"},
            metadata=MetadataSettings(discover_domain=True, discover_environments=["prod"]),
        )
    """
    dashboard = """
        from cds import DashboardSettings, DataProductConfig

        config = DataProductConfig(name="orders", dashboard=DashboardSettings(enabled=True))
    """
    files = {**PROJECT, "sales/sales_config.py": group, "sales/orders/orders_config.py": dashboard}

    assert main(["validate", str(make_project(files))]) == 0
    assert "+ Dashboard: 2 table(s), 2 page(s), in the Discover domain(s) Ventes in prod" in capsys.readouterr().out

    every = group.replace(', discover_environments=["prod"]', "")
    assert main(["validate", str(make_project({**files, "sales/sales_config.py": every}, name="every"))]) == 0
    assert "page(s), in the Discover domain(s) Ventes\n" in capsys.readouterr().out
