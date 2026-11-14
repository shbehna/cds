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
        "values": ["public", "interne", "confidentiel", "restreint"],
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
        (table, f"adresse_ligne_{line}"): "restreint"
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
        if prop.classification == "restreint" and prop.data_type.upper() != "STRING"
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
