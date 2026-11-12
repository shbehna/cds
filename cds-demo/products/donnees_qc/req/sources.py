"""Reading of the REQ open data files, shared by the contracts of the data product."""

import posixpath

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, trim, upper, when

from cds import RuntimeConfiguration

# Runtime parameter holding the folder of the files, typically a Unity Catalog volume.
SOURCE_PATH = "source_path"
# Runtime parameter: when true, the synthetic data set (cds-demo/csv/testdata) is read
# instead of the files of the register. Its files are in the same folder, named after
# those of the register: Entreprise_testdata.csv for Entreprise.csv.
SYNTHETIC_DATA = "synthetic_data"
SYNTHETIC_SUFFIX = "_testdata"


def uses_synthetic_data(runtime_configuration: RuntimeConfiguration) -> bool:
    """Whether the transformations read the synthetic data set (false by default)."""
    return runtime_configuration.get_bool(SYNTHETIC_DATA)


def source_file_name(runtime_configuration: RuntimeConfiguration, file_name: str) -> str:
    """Returns the name of the file to read: the file of the register, or its synthetic version."""
    if not uses_synthetic_data(runtime_configuration):
        return file_name
    stem, extension = posixpath.splitext(file_name)
    return f"{stem}{SYNTHETIC_SUFFIX}{extension}"


def read_source(spark: SparkSession, runtime_configuration: RuntimeConfiguration, file_name: str) -> DataFrame:
    """Reads a file of the data set: CSV encoded in UTF-8, with a header line.

    The file is that of the register, or its synthetic version when the runtime
    parameter synthetic_data is true. All the values are read as strings: the
    transformation service casts them to the types of the contract.
    """
    folder = runtime_configuration.require(SOURCE_PATH).rstrip("/")
    path = f"{folder}/{source_file_name(runtime_configuration, file_name)}"
    source = spark.read.csv(path, header=True, encoding="UTF-8", quote='"', escape='"')
    # The files start with a byte order mark, which may remain in the name of the first column.
    return source.toDF(*(name.lstrip("\ufeff") for name in source.columns))


def indicator(column: str) -> Column:
    """Converts an indicator of the register, O (oui) or N (non), to a boolean; other values become null."""
    value = upper(trim(col(column)))
    return when(value == "O", True).when(value == "N", False)
