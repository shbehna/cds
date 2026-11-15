"""Tests of the deployment manifest of the demo, without Spark nor workspace."""

import json
import textwrap
from pathlib import Path
from typing import Any

import manifest
import pytest

from cds import ConfigurationError, DataContext


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(manifest.PRODUCTS)


def test_the_committed_manifest_is_up_to_date(data_context: DataContext) -> None:
    committed = manifest.MANIFEST.read_text(encoding="utf-8")

    assert committed == manifest.render(data_context), (
        "The manifest does not match the contracts: run uv run python cds-demo/manifest/manifest.py"
    )


def test_classification(data_context: DataContext) -> None:
    document: dict[str, Any] = json.loads(manifest.render(data_context))

    assert document["data_product"] == "donnees_qc.req"
    assert document["classification"] == {
        "tag": "classification",
        "values": ["public", "internal", "confidential", "restricted"],
    }


def test_the_classified_columns_use_the_declared_values(data_context: DataContext) -> None:
    values = manifest.classification(data_context)["values"]
    classified = {
        (table.name, prop.column): prop.classification
        for table in data_context.get_data_product(manifest.DATA_PRODUCT).tables
        for prop in table.columns
        if prop.classification
    }

    # The addresses: of the establishments, of the home of the enterprises, and of the linked enterprises.
    assert classified == {
        (table, f"adresse_ligne_{line}"): "restricted"
        for table in ("entreprise", "etablissement", "fusion_scission")
        for line in (1, 2, 3, 4)
    }
    assert set(classified.values()) <= set(values)


def test_the_masked_columns_are_strings(data_context: DataContext) -> None:
    """The masking function of the deployment (cds-demo/deploy) takes and returns a string."""
    not_strings = [
        f"{table.name}.{prop.column} ({prop.data_type})"
        for table in data_context.get_data_product(manifest.DATA_PRODUCT).tables
        for prop in table.columns
        if prop.classification == "restricted" and prop.data_type.upper() != "STRING"
    ]

    assert not_strings == []


def write(root: Path, files: dict[str, str]) -> Path:
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")
    return root


CONTRACT = """
    apiVersion: v3.2.0
    kind: DataContract
    id: c
    version: 1.0.0
    schema:
      - name: {name}
        properties:
          - {{name: a, physicalType: STRING}}
"""


def test_the_classifications_must_be_declared(tmp_path: Path) -> None:
    root = write(tmp_path, {"donnees_qc/req/t/t.yaml": CONTRACT.format(name="t")})

    with pytest.raises(ConfigurationError, match="classifications is not set"):
        manifest.classification(DataContext.discover(root))


def test_the_classifications_must_be_those_of_the_data_product(tmp_path: Path) -> None:
    root = write(
        tmp_path,
        {
            "donnees_qc/req/req_config.py": """
                from cds import DataProductConfig, MetadataSettings

                config = DataProductConfig(name="req", metadata=MetadataSettings(classifications=["public"]))
            """,
            "donnees_qc/req/t/t.yaml": CONTRACT.format(name="t"),
            "donnees_qc/req/u/u.yaml": CONTRACT.format(name="u"),
            "donnees_qc/req/u/u_config.py": """
                from cds import ContractConfig, MetadataSettings

                config = ContractConfig(name="u", metadata=MetadataSettings(classifications=["secret"]))
            """,
        },
    )

    with pytest.raises(ConfigurationError, match=r"donnees_qc\.req\.u: the classification tag and values"):
        manifest.classification(DataContext.discover(root))


def test_the_job_runs_on_the_frequency_of_the_register(data_context: DataContext) -> None:
    # The register is published twice a month.
    assert manifest.schedule(data_context) == {"cron": "0 6 1,15 * *", "quartz": "0 0 6 1,15 * ?"}


def test_the_alerts_check_the_error_rate_and_the_latency(data_context: DataContext) -> None:
    alerts = {alert["name"]: alert for alert in manifest.build(data_context)["alerts"]}
    tables = sorted(table.name for table in data_context.get_data_product(manifest.DATA_PRODUCT).tables)

    assert sorted(alerts) == ["error-rate", "latency"]
    # The error rate after each load, the latency every day.
    assert (alerts["error-rate"]["cron"], alerts["error-rate"]["quartz"]) == ("0 12 1,15 * *", "0 0 12 1,15 * ?")
    assert (alerts["latency"]["cron"], alerts["latency"]["quartz"]) == ("0 7 * * *", "0 0 7 * * ?")
    for alert in alerts.values():
        assert all(f"'{table}' AS table_name" in alert["query"] for table in tables)
        assert "END AS exceeded" in alert["query"]
    error_rate = alerts["error-rate"]["query"]
    assert error_rate.count("1.0 AS threshold") == len(tables)
    assert "count_if(coalesce(size(`_errors`), 0) > 0)" in error_rate
    assert "FROM `${environment}`.`donnees_qc_req`.`entreprise`" in error_rate
    latency = alerts["latency"]["query"]
    assert latency.count("current_timestamp() - INTERVAL '18' DAY") == len(tables)
    assert "FROM `${environment}`.`information_schema`.`tables` WHERE table_schema = 'donnees_qc_req'" in latency


def test_the_alerts_are_sent_to_the_support_channel(data_context: DataContext) -> None:
    assert manifest.recipients(data_context) == ["donnees-qc-req@example.com"]


SLA_CONTRACT = """
    apiVersion: v3.2.0
    kind: DataContract
    id: {name}
    version: 1.0.0
    schema:
      - name: {name}
        properties:
          - {{name: a, physicalType: STRING}}
          - {{name: updated_at, physicalType: TIMESTAMP}}
    slaProperties:
      - {{property: frequency, value: 1, unit: d, scheduler: cron, schedule: "{cron}"}}
      - {{property: ly, value: 2, unit: h, element: {name}.updated_at, scheduler: cron, schedule: "0 * * * *"}}
"""


def test_the_tables_must_share_the_frequency(tmp_path: Path) -> None:
    root = write(
        tmp_path,
        {
            "donnees_qc/req/t/t.yaml": SLA_CONTRACT.format(name="t", cron="0 6 * * *"),
            "donnees_qc/req/u/u.yaml": SLA_CONTRACT.format(name="u", cron="0 8 * * *"),
        },
    )

    with pytest.raises(ConfigurationError, match=r"share the schedule of their frequency SLA: t \(0 6 \* \* \*\), u"):
        manifest.schedule(DataContext.discover(root))


def test_the_latency_of_a_column_is_its_latest_value(tmp_path: Path) -> None:
    root = write(tmp_path, {"donnees_qc/req/t/t.yaml": SLA_CONTRACT.format(name="t", cron="0 6 * * *")})

    alert = manifest.latency_alert(DataContext.discover(root))

    assert alert is not None and alert["quartz"] == "0 0 * * * ?"
    # The latest value of the column, instead of the last change of the table.
    assert (
        "FROM (SELECT CAST(max(`updated_at`) AS TIMESTAMP) AS updated_at FROM `${environment}`.`donnees_qc_req`.`t`)"
        in alert["query"]
    )
    assert "information_schema" not in alert["query"]
    assert "'2 hours' AS latency" in alert["query"] and "INTERVAL '2' HOUR" in alert["query"]


def test_an_alert_needs_a_cron_schedule(tmp_path: Path) -> None:
    contract = SLA_CONTRACT.format(name="t", cron="0 6 * * *").replace(
        "      - {property: frequency", "      - {property: errorRate, value: 1}\n      - {property: frequency"
    )
    root = write(tmp_path, {"donnees_qc/req/t/t.yaml": contract})

    with pytest.raises(ConfigurationError, match="the errorRate SLA needs a cron schedule"):
        manifest.error_rate_alert(DataContext.discover(root))
