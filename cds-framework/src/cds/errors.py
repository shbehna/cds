"""Exceptions raised by the CDS framework."""


class CdsError(Exception):
    """Base class of every error raised by the framework."""


class ConfigurationError(CdsError):
    """The project layout, a configuration file or a runtime parameter is invalid."""


class ContractError(CdsError):
    """A data contract is invalid, or the data does not match its contract."""


class InjectionError(CdsError):
    """A parameter of a consumer function cannot be injected."""


class ServiceError(CdsError):
    """A service completed with at least one table in error."""
