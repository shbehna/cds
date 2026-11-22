"""Configuration of the conseiller_arrondissement contract (source file ARR.csv, columns con01 to con75)."""

from pyspark.sql import DataFrame, SparkSession

from cds import ContractConfig, RuntimeConfiguration

from ...sources import read_source
from ..sources import councillors


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Turns the seats of the councils of ARR.csv into rows, one per seat with a councillor."""
    source = read_source(spark, runtime_configuration, "ARR.csv")
    return councillors(source, code="arrcod", code_alias="code_geographique")


config = ContractConfig(name="conseiller_arrondissement", transform=transform)
