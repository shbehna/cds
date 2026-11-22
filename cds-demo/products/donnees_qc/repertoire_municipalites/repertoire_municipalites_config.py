"""Data product of the directory of the municipalities of Québec (Répertoire des municipalités).

The data product is described by its ODPS document, repertoire_municipalites.odps.yaml, and
the service levels of its contracts by its partial data contract,
repertoire_municipalites.odcs.yaml.
"""

from cds import DashboardSettings, DataProductConfig, GenieSettings, ScdSettings

from .questions import EXAMPLES, INSTRUCTIONS, SAMPLE_QUESTIONS

config = DataProductConfig(
    name="repertoire_municipalites",
    # The directory publishes one row per key, but the synthetic data set of nprod has rows
    # sharing a key, which the quality service reports. The history keeps one row per key
    # instead of failing.
    scd=ScdSettings(on_duplicate_keys="deduplicate"),
    # The Genie space is generated from the contracts; these settings add how to read the
    # seats of the councils, the organisations of the positions and the unknown values, and
    # questions. The deployment masks the restricted columns: this data product has none.
    genie=GenieSettings(
        enabled=True,
        instructions=INSTRUCTIONS,
        sample_questions=SAMPLE_QUESTIONS,
        examples=EXAMPLES,
        masked_classifications=["restricted"],
    ),
    # The quality dashboard is generated from the contracts.
    dashboard=DashboardSettings(enabled=True, title="Répertoire des municipalités data quality"),
)
