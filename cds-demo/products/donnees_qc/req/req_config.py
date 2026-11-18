"""Data product of the enterprise register of Québec (Registraire des entreprises, REQ).

The defaults of its contracts (domain, status, service levels, support, usage) are in its
partial data contract, req.odcs.yaml.
"""

from cds import DataProductConfig, ScdSettings

config = DataProductConfig(
    name="req",
    # The comment of the schema of the data product.
    description=(
        "Registre des entreprises du Québec : les entreprises, leurs noms, leurs établissements, leurs fusions, "
        "scissions, continuations et transformations, tirés du jeu de données ouvertes du Registraire des "
        "entreprises."
    ),
    # The register publishes a few rows sharing the same key, which the quality service
    # reports. The history keeps one row per key instead of failing.
    scd=ScdSettings(on_duplicate_keys="deduplicate"),
)
