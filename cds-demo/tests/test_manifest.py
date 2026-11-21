"""Tests of the deployment manifest of the demo, without Spark nor workspace.

The tests read what they expect from the contracts and their configurations: changing a
contract only needs regenerating the manifest, not changing the tests.
"""

import ast
import json
import textwrap
from pathlib import Path
from typing import Any

import manifest
import pytest
import yaml

from cds import ConfigurationError, DataContext, DataProduct, MetadataSettings, Table
from cds.config import resolved
from cds.odcs import SchemaProperty
from cds.services.tags import DOMAIN_SEPARATOR, discover_domain, expected_tags
from cds.sla import cron_schedule, quartz_cron, support_emails, table_sla

# The Pulumi program and its stacks.
DEPLOY = manifest.PRODUCTS.parent / "deploy"


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(manifest.PRODUCTS)


@pytest.fixture(scope="module")
def data_product(data_context: DataContext) -> DataProduct:
    return data_context.get_data_product(manifest.DATA_PRODUCT)


def test_the_committed_manifest_is_up_to_date(data_context: DataContext) -> None:
    committed = manifest.MANIFEST.read_text(encoding="utf-8")

    assert committed == manifest.render(data_context), (
        "The manifest does not match the contracts: run uv run python cds-demo/manifest/manifest.py"
    )


def test_classification(data_context: DataContext, data_product: DataProduct) -> None:
    document: dict[str, Any] = json.loads(manifest.render(data_context))
    values = document["classification"]["values"]

    assert document["data_product"] == data_product.qualified_name
    assert values == list(resolved(data_product.settings(MetadataSettings).classifications) or [])
    assert values and len(set(values)) == len(values)


def test_the_tables_are_in_discover_domains(data_product: DataProduct) -> None:
    for table in data_product.tables:
        settings = table.settings(MetadataSettings)
        domain = discover_domain(table)
        if resolved(settings.discover_domain):
            assert domain, table.name
            assert expected_tags(table).table[domain] == "", table.name
        # The tables are in their domain in the environments of the Discover page, every
        # environment when none is set; their copies of the other environments are not.
        environments = resolved(settings.discover_environments)
        for environment in ("nprod", "prod"):
            expected = domain if environments is None or environment in environments else None
            assert discover_domain(table, environment) == expected, (table.name, environment)


def test_the_domains_are_listed_parents_first(data_context: DataContext, data_product: DataProduct) -> None:
    listed = manifest.domains(data_context)
    tags = [domain["tag"] for domain in listed]

    assert {discover_domain(table) for table in data_product.tables} - {None} <= set(tags)
    for index, domain in enumerate(listed):
        parent = domain["parent"]
        assert parent == ((domain["tag"] or "").rpartition(DOMAIN_SEPARATOR)[0] or None), domain
        assert parent is None or parent in tags[:index], domain


def classified(data_product: DataProduct) -> list[tuple[Table, SchemaProperty]]:
    return [(table, prop) for table in data_product.tables for prop in table.columns if prop.classification]


def test_the_classified_columns_use_the_declared_values(data_product: DataProduct) -> None:
    values = resolved(data_product.settings(MetadataSettings).classifications) or []

    unknown = [
        f"{table.name}.{prop.column} ({prop.classification})"
        for table, prop in classified(data_product)
        if prop.classification not in values
    ]

    assert unknown == []


def deployment_masked_classifications() -> set[str]:
    """The classifications the deployment masks: those of its stacks masking the columns.

    A stack masks the classifications of its ``cds-demo:maskedClassifications``, or by
    default those of ``pipeline.Settings``, read without importing the Pulumi program.
    """
    tree = ast.parse((DEPLOY / "pipeline.py").read_text(encoding="utf-8"))
    [default] = [
        node.value
        for definition in tree.body
        if isinstance(definition, ast.ClassDef) and definition.name == "Settings"
        for node in definition.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "masked_classifications"
        and node.value is not None
    ]
    masked: set[str] = set()
    stacks = sorted(DEPLOY.glob("Pulumi.*.yaml"))
    assert stacks
    for stack in stacks:
        config: dict[str, Any] = (yaml.safe_load(stack.read_text(encoding="utf-8")) or {}).get("config") or {}
        if str(config.get("cds-demo:masking", "false")).lower() != "true":
            continue
        value = config.get("cds-demo:maskedClassifications")
        if isinstance(value, str):
            value = json.loads(value)
        masked.update(ast.literal_eval(default) if value is None else value)
    return masked


def test_the_masked_columns_are_strings(data_product: DataProduct) -> None:
    """The masking function of the deployment (cds-demo/deploy) takes and returns a string."""
    masked = deployment_masked_classifications()

    not_strings = [
        f"{table.name}.{prop.column} ({prop.data_type})"
        for table, prop in classified(data_product)
        if prop.classification in masked and prop.data_type.upper() != "STRING"
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


def schedules(tables: list[Table], name: str) -> dict[str, str]:
    """The cron schedule of an SLA of the tables, by table name."""
    found = {table.name: table_sla(table, name) for table in tables}
    return {table: cron_schedule(sla[0]) or "" for table, sla in found.items() if sla is not None}


def test_the_job_runs_on_the_frequency_of_the_tables(data_context: DataContext, data_product: DataProduct) -> None:
    crons = set(schedules(data_product.tables, "frequency").values())

    if not crons:
        assert manifest.schedule(data_context) is None
        return
    [cron] = crons
    assert manifest.schedule(data_context) == {"cron": cron, "quartz": quartz_cron(cron)}


def test_the_alerts_check_the_sla_of_the_tables(data_context: DataContext, data_product: DataProduct) -> None:
    alerts = manifest.build(data_context)["alerts"]
    slas = {sla: schedules(data_product.tables, sla) for sla in ("errorRate", "latency")}

    assert {alert["sla"] for alert in alerts} == {sla for sla, found in slas.items() if found}
    for alert in alerts:
        found = slas[alert["sla"]]
        [cron] = set(found.values())
        assert (alert["cron"], alert["quartz"]) == (cron, quartz_cron(cron))
        # A row per table with the SLA.
        assert all(f"'{table}' AS table_name" in alert["query"] for table in found)
        assert f"END AS {manifest.EXCEEDED}" in alert["query"]
        assert "${environment}" in alert["query"]


def test_the_alerts_are_sent_to_the_support_channels(data_context: DataContext, data_product: DataProduct) -> None:
    addresses = {
        address
        for contract in data_product.contracts
        if contract.document
        for address in support_emails(contract.document)
    }

    assert set(manifest.recipients(data_context)) == addresses


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
