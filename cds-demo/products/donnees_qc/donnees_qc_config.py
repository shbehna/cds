"""Group of the data products built from the open data of Données Québec.

The defaults of the contracts of the group (tenant, tags) are in its partial data
contract, donnees_qc.odcs.yaml.
"""

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
    # The classification of the columns becomes their classification tag, a governed tag
    # whose values are these (cds-demo/manifest): the columns classified restricted are
    # masked by the deployment. The domain of the contracts, and their subdomain custom
    # property, are the Discover domain of their tables, which the deployment creates: in
    # prod only, so that the Discover page shows the tables, the Genie space and the
    # dashboard of prod, and not their nprod copies.
    metadata=MetadataSettings(
        certification={"active": "certified", "deprecated": "deprecated", "retired": "deprecated"},
        classifications=["public", "internal", "confidential", "restricted"],
        discover_domain=True,
        discover_environments=["prod"],
    ),
)
