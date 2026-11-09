# CDS framework

CDS (Contract Driven Services) builds data products on Databricks from data contracts
written with the [Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/latest/).
You describe each table in an ODCS contract and, when needed, in a small Python
configuration file. The services then create and document the tables, load them,
check their quality with [DQX](https://databrickslabs.github.io/dqx/) and keep their
history.

CDS favors convention over configuration: a setting defined for a group of data
products applies to all its data products and contracts, unless a more specific level
overrides it.

## Installation

```bash
pip install "cds-framework[dqx]"
```

The `dqx` extra is required by the quality service. PySpark is not a dependency: it is
provided by the Databricks runtime, or by Databricks Connect when running locally.

## Project layout

Data products are organized in three levels: group, data product and contract. A
project has one or more root folders:

```text
<root>/
└── <group>/
    ├── <group>_config.py                   optional
    └── <data_product>/
        ├── <data_product>_config.py        optional
        └── <contract>/
            ├── <contract>.yaml             ODCS data contract
            └── <contract>_config.py        optional
```

- A **group** gathers data products: an organization unit, a source, or a medallion
  layer if you use one.
- A **data product** is published, by default, in its own Unity Catalog schema.
- A **contract** is an ODCS document describing one or more tables (`schema` objects in
  the ODCS vocabulary).

Names must be valid identifiers (letters, digits and underscores). Other files and
folders are ignored, so configuration files can share helper modules with relative
imports, for example `from ..sources import read_source`.

## Configuration

Each configuration file defines a module-level `config` object, whose `name` must match
its folder:

```python
# donnees_qc/req/entreprise/entreprise_config.py
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from cds import ContractConfig, RuntimeConfiguration


def transform(spark: SparkSession, runtime_configuration: RuntimeConfiguration) -> DataFrame:
    source = spark.read.csv(f"{runtime_configuration.require('source_path')}/Entreprise.csv", header=True)
    # The result must have the columns of the contract; they are cast to their contract types.
    return source.select(
        col("NEQ").alias("numero_entreprise"),
        col("DAT_IMMAT").alias("date_immatriculation"),
    )


config = ContractConfig(name="entreprise", transform=transform)
```

The classes are `GroupConfig`, `DataProductConfig` and `ContractConfig`. They can be
used as is, extended with your own attributes and methods (a subclass can define
`transform` as a method), or replaced by any object with the same attributes.

Parameters are injected by name first (see `ContractInjection`), so type annotations
are optional. `cds validate` imports the configurations without a Spark session: they
may import PySpark, but must build their columns and DataFrames in functions, as above,
not when the module is loaded.

### Settings and conventions

The settings of the services are grouped by service. Each level may set some of them;
for each attribute, the most specific level wins (contract, then data product, then
group, then the framework default).

| Settings | Attribute | Default |
| --- | --- | --- |
| `NamingSettings` | `catalog` | `{environment}` |
| | `schema` | `{group}_{data_product}` |
| `SchemaSettings` | `on_breaking_change` | `replace` (or `error`) |
| `QualitySettings` | `enabled` | `True` |
| | `criticality` | `error` for every kind of inferred check |
| | `errors_column`, `warnings_column`, `info_column` | `_errors`, `_warnings`, `_dq_info` |
| `ScdSettings` | `enabled` | `True` (tables with a primary key) |
| | `table` | `{table}_history` |
| | `on_duplicate_keys` | `fail` (or `deduplicate`) |
| | `valid_from_column`, `valid_to_column`, `current_column`, `hash_column` | `_valid_from`, `_valid_to`, `_is_current`, `_row_hash` |
| `SnapshotSettings` | `enabled` | `False` |
| | `source` | the SCD history table |
| | `order_by` | `_valid_from DESC` |
| | `condition` | none |

```python
# donnees_qc/req/req_config.py
from cds import DataProductConfig, QualitySettings, ScdSettings

config = DataProductConfig(
    name="req",
    quality=QualitySettings(criticality={"foreign_key": "warn"}),
    scd=ScdSettings(on_duplicate_keys="deduplicate"),
)
```

Naming patterns use the placeholders `{environment}`, `{group}`, `{data_product}` and
`{contract}`.

### Contract defaults

Group and data product configurations may define `contract_defaults`, a partial ODCS
document merged under each of their contracts: mappings are merged key by key and the
most specific level wins. This avoids repeating the `domain`, `tenant`, `tags`,
`team`, `support` or usage terms of every contract.

## Data contracts

Contracts are ODCS v3 documents (`apiVersion: v3.0.0` to `v3.2.0`). CDS reads:

- `schema` objects as tables, named by their `physicalName` or `name`;
- their `properties` as columns, with their `physicalType` (a Databricks SQL type), or
  a type inferred from their `logicalType`, `description`, `required`, `unique`,
  `primaryKey` and `primaryKeyPosition`, `partitioned` and `partitionKeyPosition`,
  `enum` and `logicalTypeOptions`;
- `relationships` of type `foreignKey`, with the shorthand notation
  `[[<group>.]<data_product>.]<table>.<property>`, or `schema/<id>/properties/<id>` in
  the same contract;
- `quality` rules (see the quality service).

Everything else remains available in the `raw` attribute of the object model
(`cds.odcs`).

## Environments

Services run in an environment identified by a short id, such as `nprod` or `prod`.
The id is the `environment` runtime parameter and is available to naming patterns: by
default, each environment has its own catalog.

## Components

- **`ServiceContext`** bootstraps the services: it knows the environment, holds the
  Spark session, the data context and the runtime configuration, and resolves the
  physical names of the tables.
- **`DataContext`** holds the known groups, data products, contracts and tables. It
  resolves table references, and raises an error when a name is ambiguous.
- **`RuntimeConfiguration`** holds the parameters of a run as a single mapping, whether
  they come from notebook widgets, job parameters, `CDS_*` environment variables or
  arguments.
- **`ContractInjection`** calls consumer functions (transformations, dynamic quality
  checks) with their parameters resolved by name or type: `spark`,
  `service_context`, `data_context`, `runtime_configuration`, `environment`, and the
  `table`, `contract` and `data_product` being processed. A parameter matching several
  objects raises an error.
- **`cds.odcs`** is the object model of the contracts.

## Services

Each service processes the tables of a data product, in parallel, and reports a status
per table: success, skipped, warning or error. A run fails if a table is in error.

| Service | What it does |
| --- | --- |
| `schema` | Creates the schema and the tables of the contracts. Adds the columns added to a contract. Replaces a table whose columns were removed or retyped, or reports an error, depending on `on_breaking_change`. Columns are nullable: required columns are checked by the quality service, row by row. |
| `metadata` | Applies the descriptions of the contracts as table and column comments, and records the contract in the table properties. |
| `transformation` | Calls the `transform` of the contract configuration, checks that its result has exactly the columns of the contract, casts them to their contract types and replaces the content of the table (`INSERT OVERWRITE ... BY NAME`). |
| `quality` | Applies DQX checks and writes their results in the `_errors` and `_warnings` columns of each row. |
| `scd` | Keeps the history (SCD type 2) of the tables with a primary key, in `<table>_history`. |
| `snapshot` | Loads a table with the latest record of each key of a historical table. |

### Quality checks

The quality service infers checks from the contracts:

| Contract | DQX check |
| --- | --- |
| `required: true`, primary key columns | `is_not_null` |
| primary key, `unique: true` | `is_unique` |
| `enum` | `is_in_list` |
| `logicalTypeOptions` `pattern` | `regex_match` |
| `logicalTypeOptions` `minimum`, `maximum` | `is_in_range`, `is_not_less_than`, `is_not_greater_than` |
| `logicalTypeOptions` `exclusiveMinimum`, `exclusiveMaximum`, bounds of dates | `sql_expression` |
| `logicalTypeOptions` `minLength`, `maxLength`, `multipleOf` | `sql_expression` |
| `logicalTypeOptions` `minItems`, `maxItems`, `uniqueItems` (arrays) | `sql_expression` |
| `logicalTypeOptions` `required` (STRUCT and MAP), `minProperties`, `maxProperties` (MAP) | `sql_expression` |
| `logicalTypeOptions` `format` of strings: `email`, `uri`, `url`, `uuid`, `ipv4`, `ipv6`, `json` | `is_valid_email`, `is_valid_url`, `is_valid_uuid`, `is_valid_ipv4_address`, `is_valid_ipv6_address`, `is_valid_json` |
| `logicalTypeOptions` `format` of dates and timestamps stored as strings | `is_valid_date`, `is_valid_timestamp` |
| `relationships` (`foreignKey`) | `foreign_key` |
| `quality` library metrics `nullValues`, `missingValues`, `invalidValues`, `duplicateValues` that must be 0 | `is_not_null`, `is_not_null_and_not_empty`, `is_in_list` or `regex_match`, `is_unique` |
| `quality` library metric `rowCount` | `is_aggr_*` |
| `quality` rules of type `custom` with `engine: dqx` | the DQX check of their `implementation` |

A column of the primary key is not null unless the contract declares it
`required: false`. A nullable key column suits codes that the source often leaves
empty: the uniqueness check of the key then compares nulls as values
(`nulls_distinct: false`), so two rows with the same key, nulls included, are
duplicates. The `scd` service matches keys with nulls (`<=>`), and the `snapshot`
service groups them.

Null values pass the checks of `logicalTypeOptions`: a property with `required: true`
checks them. The
rules and options no check implements, such as the `format` of a DATE column, a
`timezone` or a misspelled option, are listed by `cds validate`, and logged as
warnings by the quality service.

The `quality_checks` of a contract configuration add DQX checks. They can be a
function, called with injected parameters, to compute checks at run time, for example
from a reference table. DQX reads the strings of an `allowed` list as SQL expressions
(`"open"` is a column, `"0100"` the number 100): pass the values through
`cds.literal_values` to compare them as strings.

### History tables

A history table has the columns of the contract and four technical columns: the hash
of the tracked (non-key) columns, the start and end of validity of each version, and
whether it is the current version. Each run closes the current version of the changed
and deleted keys, and inserts a version for the new and changed keys.

## Running the services

The `notebooks` folder contains one Databricks notebook per service. A notebook only
runs its service:

```python
from cds import SchemaService, run_service

run_service(SchemaService, spark=spark, dbutils=dbutils)
```

The runtime parameters are read from the notebook widgets or job parameters:

| Parameter | Description |
| --- | --- |
| `environment` | Environment id, e.g. `prod`. Required. |
| `roots` | Comma-separated root folders of the data products. Required. |
| `data_product` | Data product to process: `<group>.<data_product>`. Required. |
| `tables` | Comma-separated tables to process. All tables by default. |
| `max_workers` | Number of tables processed in parallel. 4 by default. |

Any other parameter is available to the configuration through the
`RuntimeConfiguration`. Outside a notebook, the parameters can be set as `CDS_*`
environment variables (`CDS_ENVIRONMENT=nprod`) or passed to `run_service`.

A typical pipeline runs `schema`, then `metadata` and `transformation`, then `quality`,
then `scd`.

## Command line

`cds validate <root>` loads the data products of a root folder, validates their
contracts and configurations, and validates the inferred checks with DQX, without
Spark. `cds checks <root> --data-product <group>.<data_product>` prints the inferred
checks.

## Development

The unit tests use mocks and run without Spark. The code is fully type-annotated and
checked with mypy in strict mode:

```bash
uv run pytest cds-framework/tests
uv run mypy
```

## Roadmap

- Injection of the DataFrames of tables by parameter name.
- Data products defined as sources, with implicit contracts.
- Unity Catalog tags from the contracts.
- Open Data Product Standard (ODPS) descriptors.
