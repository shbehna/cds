"""Data product of the enterprise register of Québec (Registraire des entreprises, REQ)."""

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
    # Merged under every contract of the data product, over the defaults of the group.
    contract_defaults={
        "domain": "registre_entreprises",
        "dataProduct": "donnees_qc.req",
        "status": "active",
        "description": {
            "usage": "Analyse des entreprises immatriculées au Québec.",
            "limitations": (
                "Source : Registraire des entreprises du Québec, jeu de données ouvertes publié sur Données "
                "Québec sous la licence CC BY-NC-SA 4.0. Les données sont mises à jour deux fois par mois et "
                "peuvent accuser un retard sur le registre."
            ),
        },
    },
)
