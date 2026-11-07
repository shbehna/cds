"""The services of the framework."""

from cds.services.base import DEFAULT_MAX_WORKERS, Service, ServiceReport, Status, TableResult
from cds.services.metadata import MetadataService
from cds.services.quality import QualityService
from cds.services.scd import ScdService
from cds.services.schema import SchemaService
from cds.services.snapshot import SnapshotService
from cds.services.transformation import TransformationService

# Services by name, as used by run_service and the notebooks.
SERVICES: dict[str, type[Service]] = {
    service.name: service
    for service in (
        SchemaService,
        MetadataService,
        TransformationService,
        QualityService,
        ScdService,
        SnapshotService,
    )
}

__all__ = [
    "DEFAULT_MAX_WORKERS",
    "SERVICES",
    "MetadataService",
    "QualityService",
    "ScdService",
    "SchemaService",
    "Service",
    "ServiceReport",
    "SnapshotService",
    "Status",
    "TableResult",
    "TransformationService",
]
