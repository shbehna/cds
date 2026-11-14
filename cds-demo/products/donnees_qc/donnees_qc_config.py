"""Group of the data products built from the open data of Données Québec."""

from cds import GroupConfig, MetadataSettings, NamingSettings, ScdSettings

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
    # The status of the contracts shows as a badge in Catalog Explorer: the tables of the
    # active contracts are certified, those of the deprecated or retired ones deprecated.
    metadata=MetadataSettings(
        certification={"active": "certified", "deprecated": "deprecated", "retired": "deprecated"},
    ),
    # Merged under every contract of the group. The tags become Unity Catalog tags, whose
    # keys cannot contain hyphens.
    contract_defaults={
        "tenant": "cds-demo",
        "tags": ["donnees_quebec", "donnees_ouvertes"],
    },
)
