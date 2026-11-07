"""Runtime configuration of a service, independent of how it was captured."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator, Mapping
from typing import Protocol

from cds.errors import ConfigurationError

logger = logging.getLogger(__name__)

# Environment variables with this prefix are runtime parameters: CDS_ENVIRONMENT=prod
# sets the "environment" parameter.
ENVIRONMENT_VARIABLE_PREFIX = "CDS_"

_TRUE = frozenset({"true", "1", "yes", "y", "on"})
_FALSE = frozenset({"false", "0", "no", "n", "off"})


class NotebookWidgets(Protocol):
    """The widgets of a Databricks notebook (``dbutils.widgets``), as used by the framework."""

    def getAll(self) -> Mapping[str, str]: ...

    def text(self, name: str, defaultValue: str, label: str | None = None) -> None: ...


class DBUtils(Protocol):
    """The Databricks utilities of a notebook (``dbutils``), as used by the framework."""

    @property
    def widgets(self) -> NotebookWidgets: ...


class RuntimeConfiguration(Mapping[str, str]):
    """The parameters a service runs with, as a read-only mapping of strings.

    Values may come from notebook widgets, job parameters, environment variables or
    explicit arguments: services only see the merged result. Keys are case insensitive
    and stored in lowercase. Empty values are ignored, because an unset notebook widget
    returns an empty string.
    """

    def __init__(self, values: Mapping[str, object] | None = None) -> None:
        self._values: dict[str, str] = {}
        for key, value in (values or {}).items():
            text = _to_text(value)
            if text:
                self._values[_normalize_key(key)] = text

    @classmethod
    def from_sources(cls, *sources: Mapping[str, object] | None) -> RuntimeConfiguration:
        """Merges sources of parameters, the first source having the highest precedence."""
        merged: dict[str, str] = {}
        for source in reversed([source for source in sources if source]):
            merged.update(cls(source))
        return cls(merged)

    def with_values(self, **values: object) -> RuntimeConfiguration:
        """Returns a copy in which the given values take precedence."""
        return RuntimeConfiguration.from_sources(values, self)

    def get(self, key: str, default: str | None = None) -> str | None:  # type: ignore[override]
        """Returns a parameter, or ``default`` when it is not set."""
        return self._values.get(_normalize_key(key), default)

    def require(self, key: str) -> str:
        """Returns a parameter that must be set.

        Raises:
            ConfigurationError: If the parameter is not set.
        """
        value = self.get(key)
        if value is None:
            raise ConfigurationError(
                f"The runtime parameter {key!r} is required. Set it as a notebook widget, a job "
                f"parameter or the environment variable {ENVIRONMENT_VARIABLE_PREFIX}{key.upper()}."
            )
        return value

    def get_int(self, key: str, default: int | None = None) -> int | None:
        """Returns a parameter converted to an integer, or ``default`` when it is not set."""
        value = self.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            raise ConfigurationError(f"The runtime parameter {key!r} must be an integer, got {value!r}.") from None

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Returns a parameter converted to a boolean, or ``default`` when it is not set."""
        value = self.get(key)
        if value is None:
            return default
        lowered = value.strip().lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
        raise ConfigurationError(f"The runtime parameter {key!r} must be a boolean, got {value!r}.")

    def get_list(self, key: str, separator: str = ",") -> list[str]:
        """Returns a parameter split into a list of non-empty, stripped items."""
        value = self.get(key)
        if value is None:
            return []
        return [item.strip() for item in value.split(separator) if item.strip()]

    def __getitem__(self, key: str) -> str:
        return self._values[_normalize_key(key)]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"RuntimeConfiguration({self._values!r})"


def widget_values(dbutils: DBUtils | None) -> dict[str, str]:
    """Returns the values of the notebook widgets, which include the job parameters."""
    if dbutils is None:
        return {}
    try:
        return dict(dbutils.widgets.getAll())
    except Exception as exc:
        logger.debug("Notebook widgets are not available: %s", exc)
        return {}


def environment_values(
    environ: Mapping[str, str] | None = None, prefix: str = ENVIRONMENT_VARIABLE_PREFIX
) -> dict[str, str]:
    """Returns the runtime parameters set as environment variables (``CDS_<NAME>``)."""
    environ = os.environ if environ is None else environ
    return {
        key[len(prefix) :]: value
        for key, value in environ.items()
        if key.upper().startswith(prefix) and len(key) > len(prefix)
    }


def _normalize_key(key: str) -> str:
    return key.strip().lower()


def _to_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value)
    return str(value).strip()
