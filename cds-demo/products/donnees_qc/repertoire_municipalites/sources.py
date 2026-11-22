"""Conversions of the values of the files of the directory, shared by the contracts of the data product.

The files are read by the reading shared by the data products of the group
(donnees_qc/sources.py).
"""

import re

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import array, col, explode, lit, lower, struct, trim, when

# The columns of the seats of a council: con1 to con75 in MUN.csv, con01 to con75 in ARR.csv.
SEAT_COLUMN = re.compile(r"con(\d+)")
# The name of a councillor of a seat without councillor.
VACANT = "poste vacant"


def text(column: str) -> Column:
    """Returns the value of a column without its surrounding spaces; empty values become null.

    Every value of the files is quoted: Spark reads the empty ones as empty strings, which
    the casts of the transformation service to dates and numbers would reject.
    """
    value = trim(col(column))
    return when(value != "", value)


def oui_non(column: str) -> Column:
    """Converts an indicator of the directory, oui or non, to a boolean; other values become null."""
    value = lower(trim(col(column)))
    return when(value == "oui", True).when(value == "non", False)


def councillors(source: DataFrame, code: str, code_alias: str) -> DataFrame:
    """Returns the councillors of the seats of a file, one row per seat with a councillor.

    The seats are the columns con1, con2... of the file; their number is the number of the
    seat. An empty seat has no row. A vacant seat (Poste vacant) has a row without name.

    Args:
        source: The file.
        code: Its column of the code of the municipality or borough of the seats.
        code_alias: The name of the code in the result.
    """
    seats = sorted(
        (int(match.group(1)), name) for name in source.columns if (match := SEAT_COLUMN.fullmatch(name.lower()))
    )
    seat = explode(
        array(*(struct(lit(number).alias("numero_siege"), text(name).alias("nom")) for number, name in seats))
    )
    name = col("siege.nom")
    vacant = lower(name) == VACANT
    return (
        source.select(text(code).alias(code_alias), seat.alias("siege"))
        .where(name.isNotNull())
        .select(
            col(code_alias).alias(code_alias),
            col("siege.numero_siege").alias("numero_siege"),
            when(~vacant, name).alias("nom"),
            vacant.alias("indicateur_poste_vacant"),
        )
    )
