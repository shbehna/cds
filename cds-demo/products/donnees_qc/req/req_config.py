"""Data product of the enterprise register of Québec (Registraire des entreprises, REQ).

The data product is described by its ODPS document, req.odps.yaml, and the service levels
of its contracts by its partial data contract, req.odcs.yaml.
"""

from cds import DataProductConfig, ScdSettings

config = DataProductConfig(
    name="req",
    # The register publishes a few rows sharing the same key, which the quality service
    # reports. The history keeps one row per key instead of failing.
    scd=ScdSettings(on_duplicate_keys="deduplicate"),
)
