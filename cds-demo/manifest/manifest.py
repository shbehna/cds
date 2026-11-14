"""Generates the deployment manifest of the REQ data product, read by the Pulumi program.

The Pulumi program (cds-demo/deploy) cannot load the data products: their configurations
import PySpark, which its environment does not install. The manifest holds what it needs
to know of the contracts:

- ``classification``: the key and the allowed values of the classification tag of the
  columns, which the deployment declares as a governed tag and masks.

The generated file is committed; regenerate it, from the root of the repository, after
changing the contracts or their configurations:

    uv run python cds-demo/manifest/manifest.py
"""

import json
from pathlib import Path
from typing import Any

from cds import DataContext, MetadataSettings
from cds.config import resolved
from cds.errors import ConfigurationError

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
MANIFEST = Path(__file__).with_name("req_manifest.json")
DATA_PRODUCT = "donnees_qc.req"


def classification(data_context: DataContext) -> dict[str, Any]:
    """The classification tag of the columns of the data product: its key and allowed values.

    Raises:
        ConfigurationError: If the classifications are not declared, or differ between tables.
    """
    data_product = data_context.get_data_product(DATA_PRODUCT)
    settings = data_product.settings(MetadataSettings)
    if not settings.classifications:
        raise ConfigurationError(f"{DATA_PRODUCT}: MetadataSettings.classifications is not set.")
    for table in data_product.tables:
        table_settings = table.settings(MetadataSettings)
        if (table_settings.classification_tag, table_settings.classifications) != (
            settings.classification_tag,
            settings.classifications,
        ):
            raise ConfigurationError(
                f"{table.qualified_name}: the classification tag and values must be those of {DATA_PRODUCT}."
            )
    return {"tag": resolved(settings.classification_tag), "values": list(settings.classifications)}


def build(data_context: DataContext) -> dict[str, Any]:
    """The manifest of the data product."""
    return {"data_product": DATA_PRODUCT, "classification": classification(data_context)}


def render(data_context: DataContext | None = None) -> str:
    """Returns the serialized manifest, as committed."""
    data_context = data_context or DataContext.discover(PRODUCTS)
    return json.dumps(build(data_context), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    MANIFEST.write_text(render(), encoding="utf-8", newline="\n")
    print(f"Wrote {MANIFEST}")
