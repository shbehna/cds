from __future__ import annotations

from typing import Any

import pytest
from fakes import FakeSpark

from cds import (
    ContractInjection,
    DataContext,
    Environment,
    InjectionError,
    RuntimeConfiguration,
    ServiceContext,
    Table,
)


class SparkSession:
    """Stands for pyspark.sql.SparkSession: annotations are matched by class name."""


@pytest.fixture
def injection(context: ServiceContext) -> ContractInjection:
    table = context.data_context.get_table("order")
    return ContractInjection(context, table=table, contract=table.contract)


def test_injects_by_name(injection: ContractInjection, context: ServiceContext, spark: FakeSpark) -> None:
    # The parameters are annotated as object, which matches no injectable object: only
    # their names resolve them.
    def function(
        spark: object,
        service_context: object,
        data_context: object,
        runtime_configuration: object,
        environment: object,
        table: object,
    ) -> tuple[Any, ...]:
        return spark, service_context, data_context, runtime_configuration, environment, table

    resolved = injection.call(function)

    assert resolved[:5] == (spark, context, context.data_context, context.runtime_configuration, context.environment)
    assert resolved[5].name == "order"


def test_injects_aliases(injection: ContractInjection, context: ServiceContext) -> None:
    assert injection.call(lambda context, runtime: (context, runtime)) == (context, context.runtime_configuration)


def test_injects_by_type(injection: ContractInjection, context: ServiceContext, spark: FakeSpark) -> None:
    def function(
        session: SparkSession, ctx: ServiceContext, data: DataContext | None, env: Environment, t: Table
    ) -> tuple[Any, ...]:
        return session, ctx, data, env, t

    resolved = injection.call(function)

    assert resolved[:4] == (spark, context, context.data_context, context.environment)
    assert resolved[4].name == "order"


def test_keeps_defaults_of_unknown_parameters(injection: ContractInjection) -> None:
    assert injection.call(lambda spark, limit=10: limit) == 10


def test_unknown_parameters_raise(injection: ContractInjection) -> None:
    def function(spark: object, source_table: object) -> None:
        return None

    with pytest.raises(InjectionError, match="cannot inject the parameter 'source_table'"):
        injection.call(function)


def test_ambiguous_types_raise(context: ServiceContext) -> None:
    injection = ContractInjection(context, first=RuntimeConfiguration(), second=RuntimeConfiguration())

    def function(configuration: RuntimeConfiguration) -> RuntimeConfiguration:
        return configuration

    with pytest.raises(InjectionError, match="ambiguous"):
        injection.call(function)


def test_methods_and_variadic_parameters(injection: ContractInjection, spark: FakeSpark) -> None:
    class Config:
        def transform(self, spark: object, *args: object, **kwargs: object) -> object:
            return spark

    assert injection.call(Config().transform) is spark


def test_positional_only_parameters_raise(injection: ContractInjection) -> None:
    def function(spark: object, /) -> object:
        return spark

    with pytest.raises(InjectionError, match="positional-only"):
        injection.call(function)


def test_needs_spark(context: ServiceContext) -> None:
    injection = ContractInjection(context)

    def by_name(spark: object) -> None: ...

    def by_type(session: SparkSession) -> None: ...

    def without(data_context: DataContext, table: Table | None = None) -> None: ...

    assert injection.needs_spark(by_name)
    assert injection.needs_spark(by_type)
    assert not injection.needs_spark(without)


def test_spark_is_not_injected_without_a_session(context: ServiceContext) -> None:
    offline = ServiceContext(spark=None, environment="dev", data_context=context.data_context)

    def function(spark: object) -> None: ...

    with pytest.raises(InjectionError, match="cannot inject the parameter 'spark'"):
        ContractInjection(offline).call(function)
    assert ContractInjection(offline).call(lambda data_context: data_context) is context.data_context
