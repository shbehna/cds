"""Reading of the REQ open data files, shared by the contracts of the data product."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cds import RuntimeConfiguration

# Runtime parameter holding the folder of the files, typically a Unity Catalog volume.
SOURCE_PATH = "source_path"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def read_source(spark: SparkSession, runtime_configuration: RuntimeConfiguration, file_name: str) -> DataFrame:
    """Reads a file of the data set: CSV encoded in UTF-8, with a header line.

    All the values are read as strings: the transformation service casts them to the
    types of the contract.
    """
    folder = runtime_configuration.require(SOURCE_PATH).rstrip("/")
    source = spark.read.csv(f"{folder}/{file_name}", header=True, encoding="UTF-8", quote='"', escape='"')
    # The files start with a byte order mark, which may remain in the name of the first column.
    return source.toDF(*(name.lstrip("﻿") for name in source.columns))


def indicator(column: str) -> str:
    """SQL expression converting an indicator of the register, O (oui) or N (non), to a boolean."""
    return f"CASE upper(trim(`{column}`)) WHEN 'O' THEN TRUE WHEN 'N' THEN FALSE END"


def select_columns(source: DataFrame, columns: Mapping[str, str]) -> DataFrame:
    """Selects source columns, or SQL expressions, under the names of the contract.

    Args:
        source: The DataFrame of a file.
        columns: Expression of each column of the contract: a source column name, or SQL.
    """
    return source.selectExpr(*(f"{_expression(value)} AS `{name}`" for name, value in columns.items()))


def _expression(value: str) -> str:
    return f"`{value}`" if _IDENTIFIER.match(value) else value
