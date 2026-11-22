"""Configuration of the conseiller_municipal contract (source file MUN.csv, columns con1 to con75)."""

from pyspark.sql import DataFrame, SparkSession

from cds import ContractConfig, RuntimeConfiguration

from ...sources import read_source
from ..sources import councillors


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    """Turns the seats of the councils of MUN.csv into rows, one per seat with a councillor."""
    source = read_source(spark, runtime_configuration, "MUN.csv")
    return councillors(source, code="mcode", code_alias="code_geographique")


config = ContractConfig(name="conseiller_municipal", transform=transform)
