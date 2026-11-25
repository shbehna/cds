"""The services of the framework."""

from cds.services.base import DEFAULT_MAX_WORKERS, Service, ServiceReport, Status, TableResult
from cds.services.dashboard import DashboardService
from cds.services.genie import GenieService
from cds.services.metadata import MetadataService
from cds.services.metrics import MetricsService
from cds.services.pages import PagesService
from cds.services.quality import QualityService
from cds.services.scd import ScdService
from cds.services.schema import SchemaService
from cds.services.snapshot import SnapshotService
from cds.services.transformation import TransformationService

__all__ = [
    "DEFAULT_MAX_WORKERS",
    "DashboardService",
    "GenieService",
    "MetadataService",
    "MetricsService",
    "PagesService",
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
