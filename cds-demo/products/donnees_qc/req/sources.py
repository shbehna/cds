"""Conversions of the values of the REQ open data files, shared by the contracts of the data product.

The files are read by the reading shared by the data products of the group
(donnees_qc/sources.py).
"""

from pyspark.sql import Column
from pyspark.sql.functions import col, trim, upper, when


def indicator(column: str) -> Column:
    """Converts an indicator of the register, O (oui) or N (non), to a boolean; other values become null."""
    value = upper(trim(col(column)))
    return when(value == "O", True).when(value == "N", False)
