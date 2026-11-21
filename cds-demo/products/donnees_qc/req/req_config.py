"""Data product of the enterprise register of Québec (Registraire des entreprises, REQ).

The data product is described by its ODPS document, req.odps.yaml, and the service levels
of its contracts by its partial data contract, req.odcs.yaml.
"""

from cds import DashboardSettings, DataProductConfig, GenieSettings, QualitySettings, ScdSettings

from .questions import EXAMPLES, INSTRUCTIONS, SAMPLE_QUESTIONS

config = DataProductConfig(
    name="req",
    # The register uses codes missing from its value domains (allowedValues of the
    # contracts): they are reported as warnings.
    quality=QualitySettings(criticality={"allowed_values": "warn"}),
    # The register publishes a few rows sharing the same key, which the quality service
    # reports. The history keeps one row per key instead of failing.
    scd=ScdSettings(on_duplicate_keys="deduplicate"),
    # The Genie space is generated from the contracts; these settings add the value domains
    # of the codes, and questions. The deployment masks the restricted columns.
    genie=GenieSettings(
        enabled=True,
        instructions=INSTRUCTIONS,
        sample_questions=SAMPLE_QUESTIONS,
        examples=EXAMPLES,
        masked_classifications=["restricted"],
    ),
    # The quality dashboard is generated from the contracts. Published with the credentials
    # of the service principal, which the masking exempts, it leaves the restricted
    # addresses out of the keys of the rows: it only shows the public columns.
    dashboard=DashboardSettings(enabled=True, title="REQ data quality"),
)
