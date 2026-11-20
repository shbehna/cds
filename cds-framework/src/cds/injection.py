"""Injection of the parameters of consumer functions, such as transformations."""

from __future__ import annotations

import inspect
import types
import typing
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, TypeVar

from cds.errors import InjectionError

if TYPE_CHECKING:
    from cds.context import ServiceContext

T = TypeVar("T")

# Alternative parameter names of the injectable objects.
ALIASES: Mapping[str, str] = {"context": "service_context", "runtime": "runtime_configuration"}

_SPARK = "spark"


class ContractInjection:
    """Calls consumer functions, resolving their parameters from the service context.

    Each parameter is resolved by name, then by type annotation, among the injectable
    objects:

    - ``spark``: the Spark session;
    - ``service_context`` (or ``context``): the ``ServiceContext``;
    - ``data_context``: the ``DataContext``;
    - ``runtime_configuration`` (or ``runtime``): the ``RuntimeConfiguration``;
    - ``environment``: the ``Environment``;
    - the objects given by the service, e.g. ``table``, ``contract`` and ``data_product``.

    A parameter annotated with a type matching two or more objects is ambiguous and
    raises an ``InjectionError``. A parameter that cannot be resolved keeps its default
    value, or raises an ``InjectionError`` if it has none.

    Sources are read through the Spark session for now: the injection of table
    DataFrames by parameter name will extend this resolution.
    """

    def __init__(self, context: ServiceContext, **objects: object) -> None:
        # Without a Spark session, as in ``cds validate``, a function needing one cannot be called.
        spark: dict[str, object] = {_SPARK: context.spark} if context.has_spark else {}
        self._objects: dict[str, object] = {
            **spark,
            "service_context": context,
            "data_context": context.data_context,
            "runtime_configuration": context.runtime_configuration,
            "environment": context.environment,
            **objects,
        }

    @property
    def names(self) -> list[str]:
        """Names of the injectable objects."""
        return sorted(self._objects)

    def resolve(self, function: Callable[..., object]) -> dict[str, object]:
        """Returns the keyword arguments to call a function with.

        Raises:
            InjectionError: If a parameter cannot be resolved, or is ambiguous.
        """
        try:
            signature = inspect.signature(function)
        except (TypeError, ValueError) as exc:
            raise InjectionError(f"Cannot inspect {_describe(function)}: {exc}") from exc
        hints = _type_hints(function)
        arguments: dict[str, object] = {}
        for parameter in signature.parameters.values():
            if parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            if parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
                raise InjectionError(
                    f"{_describe(function)}: positional-only parameters cannot be injected ({parameter.name!r})."
                )
            found, value = self._resolve_parameter(function, parameter, hints.get(parameter.name))
            if found:
                arguments[parameter.name] = value
            elif parameter.default is inspect.Parameter.empty:
                raise InjectionError(
                    f"{_describe(function)}: cannot inject the parameter {parameter.name!r}. "
                    f"Injectable names: {', '.join(self.names)}."
                )
        return arguments

    def needs_spark(self, function: Callable[..., object]) -> bool:
        """Whether a function has a parameter resolved as the Spark session.

        That is a parameter named ``spark``, or typed ``SparkSession`` and not named after
        another injectable object.

        Raises:
            InjectionError: If the function cannot be inspected.
        """
        try:
            signature = inspect.signature(function)
        except (TypeError, ValueError) as exc:
            raise InjectionError(f"Cannot inspect {_describe(function)}: {exc}") from exc
        hints = _type_hints(function)
        for parameter in signature.parameters.values():
            name = ALIASES.get(parameter.name, parameter.name)
            if name == _SPARK:
                return True
            if name not in self._objects and _matches(_SPARK, None, hints.get(parameter.name)):
                return True
        return False

    def call(self, function: Callable[..., T]) -> T:
        """Calls a function with its injected parameters."""
        return function(**self.resolve(function))

    def _resolve_parameter(
        self, function: Callable[..., object], parameter: inspect.Parameter, annotation: object
    ) -> tuple[bool, object]:
        name = ALIASES.get(parameter.name, parameter.name)
        if name in self._objects:
            return True, self._objects[name]
        if annotation is None:
            return False, None
        matches = [key for key, value in self._objects.items() if _matches(key, value, annotation)]
        if len(matches) > 1:
            raise InjectionError(
                f"{_describe(function)}: the parameter {parameter.name!r} is ambiguous, its type "
                f"matches {', '.join(sorted(matches))}. Rename it to one of these names."
            )
        if matches:
            return True, self._objects[matches[0]]
        return False, None


def _type_hints(function: Callable[..., object]) -> dict[str, object]:
    try:
        return typing.get_type_hints(function)
    except Exception:
        try:
            parameters = inspect.signature(function).parameters.values()
        except (TypeError, ValueError):
            return {}
        return {
            parameter.name: parameter.annotation
            for parameter in parameters
            if parameter.annotation is not inspect.Parameter.empty and not isinstance(parameter.annotation, str)
        }


def _matches(key: str, value: object, annotation: object) -> bool:
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is types.UnionType:
        return any(_matches(key, value, member) for member in typing.get_args(annotation) if member is not type(None))
    if not isinstance(annotation, type):
        return False
    if annotation.__name__ == "SparkSession":
        # Classic and Spark Connect sessions do not share a base class.
        return key == _SPARK
    if annotation is object:
        return False
    return isinstance(value, annotation)


def _describe(function: Callable[..., object]) -> str:
    name = getattr(function, "__qualname__", None) or getattr(function, "__name__", None) or repr(function)
    module = getattr(function, "__module__", None)
    return f"{module}.{name}" if module and not module.startswith("_cds_") else name
