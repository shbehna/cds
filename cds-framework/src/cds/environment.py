"""Deployment environments."""

from __future__ import annotations

import re
from dataclasses import dataclass

from cds.errors import ConfigurationError

_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class Environment:
    """An environment the services run in, identified by a short id such as ``prod``.

    The id is available to naming patterns as ``{environment}``. By convention, each
    environment has its own Unity Catalog catalog.
    """

    id: str

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not _ID_PATTERN.match(self.id):
            raise ConfigurationError(
                f"Invalid environment id {self.id!r}: expected a short lowercase identifier such as 'prod' or 'nprod'."
            )

    def __str__(self) -> str:
        return self.id
