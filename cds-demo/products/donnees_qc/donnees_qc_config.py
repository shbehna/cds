"""Group of the data products built from the open data of Données Québec."""

from cds import GroupConfig, NamingSettings, ScdSettings

config = GroupConfig(
    name="donnees_qc",
    description=(
        "Produits de données construits à partir des données ouvertes publiées sur Données Québec (donneesquebec.ca)."
    ),
    # The framework convention, written here to show where it lives: one catalog per
    # environment (nprod, prod) and one schema per data product (donnees_qc_req).
    naming=NamingSettings(catalog="{environment}", schema="{group}_{data_product}"),
    # The tables of the group have French names: so do their history tables.
    scd=ScdSettings(table="{table}_historique"),
    # Merged under every contract of the group.
    contract_defaults={
        "tenant": "cds-demo",
        "tags": ["donnees-quebec", "donnees-ouvertes"],
    },
)
