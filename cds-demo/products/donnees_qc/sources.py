"""Reading of the open data files of Données Québec, shared by the data products of the group."""

import posixpath

from pyspark.sql import DataFrame, SparkSession

from cds import RuntimeConfiguration

# Runtime parameter holding the folder of the files, typically a Unity Catalog volume.
SOURCE_PATH = "source_path"
# Runtime parameter: when true, the synthetic data set (cds-demo/csv/testdata) is read
# instead of the published files. Its files are in the same folder, named after the
# published ones: Entreprise_testdata.csv for Entreprise.csv.
SYNTHETIC_DATA = "synthetic_data"
SYNTHETIC_SUFFIX = "_testdata"


def uses_synthetic_data(runtime_configuration: RuntimeConfiguration) -> bool:
    """Whether the transformations read the synthetic data set (false by default)."""
    return runtime_configuration.get_bool(SYNTHETIC_DATA)


def source_file_name(runtime_configuration: RuntimeConfiguration, file_name: str) -> str:
    """Returns the name of the file to read: the published file, or its synthetic version."""
    if not uses_synthetic_data(runtime_configuration):
        return file_name
    stem, extension = posixpath.splitext(file_name)
    return f"{stem}{SYNTHETIC_SUFFIX}{extension}"


def read_source(spark: SparkSession, runtime_configuration: RuntimeConfiguration, file_name: str) -> DataFrame:
    """Reads a file of a data set: CSV encoded in UTF-8, with a header line.

    The file is the published one, or its synthetic version when the runtime parameter
    synthetic_data is true. All the values are read as strings: the transformation
    service casts them to the types of the contract.
    """
    folder = runtime_configuration.require(SOURCE_PATH).rstrip("/")
    path = f"{folder}/{source_file_name(runtime_configuration, file_name)}"
    source = spark.read.csv(path, header=True, encoding="UTF-8", quote='"', escape='"')
    # The files start with a byte order mark, which may remain in the name of the first column.
    return source.toDF(*(name.lstrip("\ufeff") for name in source.columns))
