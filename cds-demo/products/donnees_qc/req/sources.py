"""Reading of the REQ open data files, shared by the contracts of the data product."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.functions import col, trim, upper, when

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

    from cds import RuntimeConfiguration

# Runtime parameter holding the folder of the files, typically a Unity Catalog volume.
SOURCE_PATH = "source_path"


def read_source(spark: SparkSession, runtime_configuration: RuntimeConfiguration, file_name: str) -> DataFrame:
    """Reads a file of the data set: CSV encoded in UTF-8, with a header line.

    All the values are read as strings: the transformation service casts them to the
    types of the contract.
    """
    folder = runtime_configuration.require(SOURCE_PATH).rstrip("/")
    source = spark.read.csv(f"{folder}/{file_name}", header=True, encoding="UTF-8", quote='"', escape='"')
    # The files start with a byte order mark, which may remain in the name of the first column.
    return source.toDF(*(name.lstrip("﻿") for name in source.columns))


def indicator(column: str) -> Column:
    """Converts an indicator of the register, O (oui) or N (non), to a boolean; other values become null."""
    value = upper(trim(col(column)))
    return when(value == "O", True).when(value == "N", False)
