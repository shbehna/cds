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
    ├── <group>.odcs.yaml                   optional partial ODCS document: contract defaults
    └── <data_product>/
        ├── <data_product>_config.py        optional
        ├── <data_product>.odcs.yaml        optional partial ODCS document: contract defaults
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
| `MetadataSettings` | `tags` | `True` |
| | `field_tags` | `domain` and `dataProduct` (tag `data_product`) |
| | `certification` | none: the tables are not certified |
| | `certification_tag` | `system.certification_status` |
| | `classification_tag` | `classification` |
| | `classifications` | none: any classification |
| | `discover_domain` | `False`: the tables are not tagged with their Discover domain |

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

A group or data product may define contract defaults: a partial ODCS document merged
under each of its contracts. Mappings are merged key by key and the most specific level
wins: the group, then the data product, then the contract itself. Any other value,
lists included, replaces that of the previous levels. This avoids repeating the
`domain`, `tenant`, `tags`, `team`, `support`, `slaProperties` or usage terms of every
contract.

The defaults of a level are written in its partial ODCS file, `<group>.odcs.yaml` or
`<data_product>.odcs.yaml` (or `.odcs.yml`), next to its configuration file:

```yaml
# donnees_qc/req/req.odcs.yaml
domain: Économie
status: active
support:
  - channel: donnees-qc-req
    tool: email
    url: "mailto:donnees-qc-req@example.com"
```

A partial document holds top-level ODCS fields, but not those identifying a contract:
`apiVersion`, `kind`, `id`, `name`, `version` and `schema`. A file holding only comments
defines no defaults. Instead of the file, the configuration of a level may define its
contract defaults in Python, as its `contract_defaults` mapping, with the same rules; a
level cannot use both.

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
- `quality` rules (see the quality service);
- `tags` of the contract, of its `schema` objects and of their properties, the
  `classification` of the properties, the `status`, `domain` and `dataProduct` of the
  contract, and its `subdomain` custom property (see the tags of the metadata service);
- `slaProperties` and `support` (see [Service levels](#service-levels)).

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
| `schema` | Creates the schema and the tables of the contracts, with the tags of their columns. Adds the columns added to a contract. Replaces a table whose columns were removed or retyped, or reports an error, depending on `on_breaking_change`. Columns are nullable: required columns are checked by the quality service, row by row. |
| `metadata` | Applies the descriptions of the contracts as table and column comments, synchronizes the Unity Catalog tags of the tables and columns with the contracts, and records the contract in the table properties. |
| `transformation` | Calls the `transform` of the contract configuration, checks that its result has exactly the columns of the contract, casts them to their contract types and replaces the content of the table (`INSERT OVERWRITE ... BY NAME`). |
| `quality` | Applies DQX checks and writes their results in the `_errors` and `_warnings` columns of each row. |
| `scd` | Keeps the history (SCD type 2) of the tables with a primary key, in `<table>_history`, whose columns get the tags of those of the table. Purges the versions closed for longer than the `retention` of the contract. |
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

### Tags

The metadata service synchronizes the Unity Catalog tags of the tables and their columns
with their contracts:

| Contract | Unity Catalog tag |
| --- | --- |
| `tags` of the contract and of the `schema` object | tags of the table |
| fields of `MetadataSettings.field_tags`, such as `domain` | tags of the table: `domain`, `data_product` |
| `domain` and the `subdomain` custom property, with `MetadataSettings.discover_domain` | governed tag of the Discover domain of the table: `<domain>` or `<domain>/<subdomain>` |
| `status`, through `MetadataSettings.certification` | certification of the table: `system.certification_status` |
| `tags` of a property | tags of its column |
| `classification` of a property | tag `classification` of its column |

A tag `key:value` becomes the tag `key` with the value `value`; a tag without colon, a
tag without value. Unity Catalog does not allow `.`, `,`, `-`, `=`, `/` and `:` in the
keys, except the `/` separating the levels of a Discover domain (see below): `cds
validate` reports the invalid tags, and the classifications not listed in
`MetadataSettings.classifications`.

The certification shows a badge in Catalog Explorer: map the statuses of the contracts
to the values of the governed tag `system.certification_status`, `certified` or
`deprecated`:

```python
from cds import GroupConfig, MetadataSettings

config = GroupConfig(
    name="donnees_qc",
    metadata=MetadataSettings(certification={"active": "certified", "deprecated": "deprecated"}),
)
```

The [Discover page](https://docs.databricks.com/aws/en/discover/discover-page) of
Databricks organizes the assets in domains and subdomains, each built on a governed tag:
the tables tagged with it belong to the domain. With `MetadataSettings.discover_domain`,
the `domain` of a contract is the Discover domain of its tables, and its `subdomain`
custom property places them in a subdomain, whose tag is its path under the domain:

```yaml
domain: Finance
customProperties:
  - property: subdomain
    value: Tax          # governed tag Finance/Tax, in the subdomain Tax of Finance
```

The tables of a subdomain also appear on the page of its parent domain. The governed
tags and the domains are not created by the framework: create them first, with the
deployment or in the **Discover** page. A domain has up to five levels, each a valid tag
key, which `cds validate` checks. The `customProperties` of a contract replace those of
the contract defaults: a contract defining its own repeats the `subdomain`.

The history table of a table gets the tags of its columns, but not those of the table
nor its certification: a policy based on the classification of the columns also protects
the history. The history tables are not in the Discover domain of their table.

The tags of the columns also protect the data from its first write: the `schema`
service tags the columns of the tables it creates or replaces and the columns it adds,
and the `scd` service those of the history tables, before any data is written to them. A
new table whose columns cannot be tagged is dropped, and its service reports an error,
so that no data is loaded into it untagged. The tags of the tables, such as the
certification, are set by the `metadata` service.

The service only changes the tags it applied, whose keys it records in the `cds.tags`
table property: it removes a tag no longer in the contract, and leaves the tags set by
other means, such as the data classification of Databricks. Setting a governed tag
requires the `ASSIGN` permission on it, and setting tags the `APPLY TAG` privilege on
the table, which its owner has. With `MetadataSettings(tags=False)`, the service does
not change the tags.

### History tables

A history table has the columns of the contract and four technical columns: the hash
of the tracked (non-key) columns, the start and end of validity of each version, and
whether it is the current version. Each run closes the current version of the changed
and deleted keys, and inserts a version for the new and changed keys.

### Service levels

The `slaProperties` of a contract state the service levels of its tables
([`cds.sla`](src/cds/sla.py)). The `scd` service applies the `retention`; the other
properties are for the deployments, which can schedule the loads (`frequency`) and alert
on stale data (`latency`) or error rates (`errorRate`), as the demo does.

```yaml
slaProperties:
  - property: retention     # the history keeps the closed versions 10 years
    value: 10
    unit: y
  - property: latency       # checked every day at 7 am
    value: 18
    unit: d
    element: orders         # a table, or a column (orders.updated_at); every table without it
    scheduler: cron
    schedule: "0 7 * * *"
support:
  - channel: data-team
    tool: email
    url: mailto:data-team@example.com
```

A property applies to the tables of its `element`; without one, to the
`slaDefaultElement` of the contract (ODCS v3.0), or else to every table. A property
whose element designates a table takes precedence over the one without element; `cds
validate` reports two properties of the same name for the same table. The `retention`
applies to the versions of the history tables: its element designates tables, not
columns. Durations are positive integers in `h`, `d`, `w`, `mo` or `y` (or
`hours`, `days`...), the `errorRate` and the `availability` percentages (`percent`).
The schedules are cron expressions of five fields, with `scheduler: cron`;
`cds.sla.quartz_cron` converts them to the Quartz syntax of the Databricks jobs and
alerts. The addresses of the `email` support channels are given by
`cds.sla.support_emails`.

`cds validate` checks the names of the properties, or their abbreviations (`ly` for
`latency`...), their values and units, their elements and their cron schedules, and the
`mailto:` url of the email channels.

The `scd` service deletes from a history table the versions closed for longer than the
`retention` of its table, and reports their number; the current versions are kept. A
contract defining `slaProperties` or `support` replaces those of the contract defaults.

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

A typical pipeline runs `schema`, then `transformation`, then `metadata` and `quality`,
then `scd`. Running `metadata` after the load keeps the time of the last change of the
tables (`last_altered`) for the loads that succeeded.

## Command line

`cds validate <root>` loads the data products of a root folder, validates their
contracts and configurations, including their tags and service levels, and validates the
inferred checks with DQX, without Spark. `cds checks <root> --data-product <group>.<data_product>` prints the inferred
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
- Open Data Product Standard (ODPS) descriptors.
