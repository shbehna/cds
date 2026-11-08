from pathlib import Path
from unittest.mock import MagicMock

import pytest
from conftest import MakeProject
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


def test_cli_validate_reports_errors(make_project: MakeProject, capsys: pytest.CaptureFixture[str]) -> None:
    root = make_project({"s/d/c/c.yaml": "apiVersion: v3.2.0\nkind: DataContract\nversion: 1\n"})

    assert main(["validate", str(root)]) == 1
    assert "id: is required" in capsys.readouterr().err


def test_cli_checks(project: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["checks", str(project), "--data-product", "sales.orders", "--environment", "prod"]) == 0

    assert '"ref_table": "`prod`.`sales_orders`.`customer`"' in capsys.readouterr().out
