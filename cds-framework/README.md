# CDS framework

CDS (Contract Driven Services) builds data products on Databricks from data contracts
written with the [Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/latest/).
You describe each table in an ODCS contract and, when needed, in a small Python
configuration file. A data product may also be described by an
[Open Data Product Standard (ODPS)](https://bitol-io.github.io/open-data-product-standard/latest/)
document. The services then create and document the tables, load them,
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
        ├── <data_product>.odps.yaml        optional ODPS document describing the data product
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
| | `discover_domain` | `False`: the tables, the Genie space and the dashboard are not in a Discover domain |
| | `discover_environments` | none: the assets of every environment are in their Discover domain |
| `GenieSettings` | `enabled` | `False`: the data product has no Genie space |
| | `title` | the `name` of the ODPS document, else `<group>.<data_product>` |
| | `instructions`, `sample_questions`, `examples` | none |
| | `masked_classifications` | none |
| `DashboardSettings` | `enabled` | `False`: the data product has no quality dashboard |
| | `title` | `<name> data quality`, the `name` of the ODPS document, else `<group>.<data_product>` |
| | `shown_classifications` | `public` |

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
wins. Any other value, lists included, replaces that of the previous levels. This avoids
repeating the `domain`, `tenant`, `tags`, `team`, `support`, `slaProperties` or usage
terms of every contract. Each contract is the merge of, from the lowest precedence to the
highest:

1. the contract defaults of its group;
2. the fields of the [ODPS document](#data-product-documents-odps) of its data product
   that contracts also have;
3. the contract defaults of its data product, which cannot set those fields again;
4. the contract itself.

The defaults of a level are written in its partial ODCS file, `<group>.odcs.yaml` or
`<data_product>.odcs.yaml` (or `.odcs.yml`), next to its configuration file:

```yaml
# donnees_qc/req/req.odcs.yaml
slaProperties:
  - property: latency
    value: 18
    unit: d
```

A partial document holds top-level ODCS fields, but not those identifying a contract:
`apiVersion`, `kind`, `id`, `name`, `version` and `schema`. A file holding only comments
defines no defaults. Instead of the file, the configuration of a level may define its
contract defaults in Python, as its `contract_defaults` mapping, with the same rules; a
level cannot use both.

## Data product documents (ODPS)

A data product may be described by an
[Open Data Product Standard (ODPS)](https://bitol-io.github.io/open-data-product-standard/latest/)
document (`apiVersion: v1.0.0` or `v1.1.0`), `<data_product>.odps.yaml` (or
`.odps.yml`). Its `id` must be `<group>.<data_product>`.

```yaml
# donnees_qc/req/req.odps.yaml
apiVersion: v1.1.0
kind: DataProduct
id: donnees_qc.req
status: active
domain: Économie
description:
  purpose: Registre des entreprises du Québec ...
  usage: Analyse des entreprises immatriculées au Québec.
support:
  - channel: donnees-qc-req
    url: "mailto:donnees-qc-req@example.com"
```

The `purpose` of its `description` is the description of the data product, the comment
of its schema: the configuration cannot also set a `description`. The fields that
contracts also have are written once in the document, and copied into each contract of
the data product (`cds.odps.contract_fields`):

| ODPS field | Copied into each contract as |
| --- | --- |
| `id` | `dataProduct` |
| `domain`, `tenant`, `status` | the same field |
| `customProperties` | the same list |
| `support` | the same list, without the fields that ODCS channels do not have (`tags`, `authoritativeDefinitions`) |
| `description.usage`, `description.limitations` | the same fields of the `description` |

The `purpose` is not copied, since each contract has its own. Neither are the `tags`:
a list replaces that of the previous levels, so they would replace the tags of the
group. The contract defaults of the data product cannot set a copied field again, and
fields that ODPS does not describe, such as the `slaProperties`, stay in them. A contract
may still set any field itself.

The document is available as `DataProduct.document` (`cds.odps`), and everything CDS
does not read, such as its ports, remains available in its `raw` attribute.

## Data contracts

Contracts are ODCS v3 documents (`apiVersion: v3.0.0` to `v3.2.0`). CDS reads:

- `schema` objects as tables, named by their `physicalName` or `name`;
- their `properties` as columns, with their `physicalType` (a Databricks SQL type), or
  a type inferred from their `logicalType`, `description`, `required`, `unique`,
  `primaryKey` and `primaryKeyPosition`, `partitioned` and `partitionKeyPosition`,
  `enum` and `logicalTypeOptions`, and the `allowedValues` custom property (see the
  quality service);
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
| `genie` | Creates the [Genie space](#genie-spaces) of the data product from its contracts, or updates it to match them. It processes the data product, not its tables, and reports a single result. |
| `dashboard` | Creates the [quality dashboard](#quality-dashboards) of the data product from its contracts, or updates it, and publishes it. It processes the data product, not its tables, and reports a single result. |

### Quality checks

The quality service infers checks from the contracts:

| Contract | DQX check |
| --- | --- |
| `required: true`, primary key columns | `is_not_null` |
| primary key, `unique: true` | `is_unique` |
| `enum` | `is_in_list` |
| `allowedValues` custom property | `is_in_list`, with values read at run time |
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

When the allowed values of a column are kept in another table, such as a table of
codes, the `allowedValues` custom property of the property names its column
(`reference`, with the notation of the relationships), and optionally a SQL expression
filtering its rows (`filter`):

```yaml
- name: activity_code
  physicalType: STRING
  customProperties:
    - property: allowedValues
      value: { reference: code.value, filter: "domain = 'ACTIVITY'" }
```

The quality service reads the distinct values, nulls excluded, when it checks the table:
the referenced table must be loaded before. If there are none, the column is not
checked, and the service logs a warning. Their criticality is that of the
`allowed_values` kind. `cds validate` checks the declaration and lists these checks as
computed at run time.

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

The tables of a subdomain also appear on the page of its parent domain. The
[Genie space](#genie-spaces) and the [quality dashboard](#quality-dashboards) of a data
product join the Discover domains of their tables: the `genie` and `dashboard` services
tag them with the same governed tags, which the user running them needs `ASSIGN` on. The
governed tags and the domains are not created by the framework: create them first, with
the deployment or in the **Discover** page.

`MetadataSettings.discover_environments` limits the domains to some environments, such
as `["prod"]`: the tables, the Genie space and the dashboard of the other environments
stay out of the Discover page, which would otherwise show them side by side with those
of production. The services remove the domain tags they applied in those environments.
`cds validate` checks the domains as in every environment. A domain has up to five levels, each a valid tag
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

## Genie spaces

A [Genie space](https://docs.databricks.com/aws/en/genie/) answers questions on tables in
natural language, by writing SQL. Genie reads the comments of the tables and columns,
which the metadata service sets from the contracts. `cds.genie.build_space` generates the
space of a data product from its contracts and its ODPS document, with what the comments
do not say:

| Genie space | From |
| --- | --- |
| Tables | the tables of the data product, in the environment of the service context |
| Joins | the `foreignKey` relationships to the tables of the data product: `MANY_TO_ONE` to a primary key or unique column, `ONE_TO_ONE` when the source columns are also the key of their table, `MANY_TO_MANY` otherwise. A second join to the same table gets an alias with the suffix of its columns: `customer_billed` for `customer_id_billed` |
| Descriptions of the coded columns | the `enum` of their properties: each value and its label |
| Instructions | the purpose, `usage` and `limitations` of the ODPS document, the primary keys, the deprecated and retired contracts, the quality result columns of the tables that have checks, the `errorRate` service level when every checked table has the same, the `STRING` columns of `masked_classifications` (a column mask on strings masks them), and the description of the `frequency` service level |

The tables are sorted by their full name, as the Genie API requires.

The `GenieSettings` of a data product, or of its group, enable its space and add what the
contracts cannot say: instructions, sample questions and examples, in the language of the
users of the space.

```python
from cds import DataProduct, DataProductConfig, GenieExample, GenieSettings, ServiceContext
from cds.genie import error_rate_sql


def open_orders(service_context: ServiceContext, data_product: DataProduct) -> str:
    table = service_context.full_table_name(data_product.get_table("order"))
    return f"SELECT count(*) FROM {table} WHERE status = 'open'"


config = DataProductConfig(
    name="orders",
    genie=GenieSettings(
        enabled=True,
        instructions=["Amounts are in euros."],
        sample_questions=["Which customers order the most?"],
        examples=[
            GenieExample("How many orders are open?", open_orders),
            GenieExample("Which tables have quality errors?", error_rate_sql),
        ],
        masked_classifications=["restricted"],
    ),
)
```

The SQL of an example is a string, or a function called with injected parameters, such
as `service_context` and `data_product`, to name the tables of the environment.
`cds.genie.error_rate_sql` returns the share of the rows of each table in error, among
the tables that have checks. The `serialized_space` of the result is the configuration of
the Genie API (version 2), which takes it as a JSON string.

`cds validate` builds the space of the data products that have one, and `cds genie`
prints it. Without Spark, they leave out the examples whose function needs the `spark`
session, and report them as computed at run time; an example whose function fails is an
error of the space.

The `genie` service creates the space in the workspace, titled `<title> (<environment>)`,
and keeps its id in the `cds.genie_space_id` property of the schema of the data product.
The next runs update that space when the contracts or the settings change, leave it
unchanged otherwise, and create a new one if it was deleted. With
`MetadataSettings.discover_domain`, the space joins the Discover domains of its tables
(see [Tags](#tags)), recorded in the `cds.genie_space_domains`
property: the next runs remove the domains it no longer has, and leave those added in
the Discover page. A space that cannot be tagged is a warning. It reads two runtime
parameters:

| Parameter | Description |
| --- | --- |
| `genie_warehouse_id` | Id of the SQL warehouse running the queries of the space. Required. |
| `genie_parent_path` | Workspace folder of the space, such as `/Workspace/Shared/genie`. The home folder of the user running the service by default. |

A space inherits the permissions of its folder: grant its users `CAN RUN` on the folder.
They then ask questions with their own permissions, on the warehouse of the space: they
need `CAN USE` on it and `SELECT` on the tables, and column masks and row filters apply.
If the id of a new space cannot be recorded in the schema, the service deletes the space
and fails, rather than creating another one on the next run. The user running the service needs the *Databricks SQL access* entitlement, `CAN
USE` on the warehouse, and the right to set the properties of the schema, which its owner
has.

## Quality dashboards

`cds.dashboard.build_dashboard` generates the quality dashboard of a data product, a
Databricks AI/BI dashboard of the results of the quality service, from its contracts:

| Dashboard | From |
| --- | --- |
| Tables | the tables that get checks (`cds.services.checks.checked_tables`), named in the environment of the service context |
| Key of the rows | the primary key of each table, as a JSON object; all the columns of a table without primary key. The classified columns not in `DashboardSettings.shown_classifications` are left out |
| Results | the `errors_column` and `warnings_column` of `QualitySettings` |

It has two pages. **Statistics** shows the status of the rows (without issue, with
warnings, with errors: a row with both is in error), the occurrences of each check, and
their count by table, check and severity. **Occurrences** lists each error and warning
with the key of its row, its check, message and columns. The `DashboardSettings` of a
data product, or of its group, enable it:

```python
from cds import DashboardSettings, DataProductConfig

config = DataProductConfig(name="orders", dashboard=DashboardSettings(enabled=True, title="Orders quality"))
```

The `dashboard` service creates it in the workspace, titled `<title> (<environment>)`,
and publishes it with embedded credentials: its viewers see the data with the
permissions of the user running the service, which a column mask may exempt, hence the
classified columns left out of the keys. The service keeps its id in the
`cds.dashboard_id` property of the schema of the data product. The next runs update the
dashboard when the contracts or the settings change and publish it again, leave it
unchanged otherwise, and create a new one if it was deleted. As the Genie space, the
dashboard joins the Discover domains of its tables, recorded in the
`cds.dashboard_domains` property. It reads two runtime parameters:

| Parameter | Description |
| --- | --- |
| `dashboard_warehouse_id` | Id of the SQL warehouse running the queries of the dashboard. Required. |
| `dashboard_parent_path` | Workspace folder of a new dashboard, such as `/Workspace/Shared/dashboards`. The home folder of the user running the service by default. An existing dashboard is not moved: the service warns when it is in another folder. |

`cds validate` builds the dashboard of the data products that have one, and `cds
dashboard` prints it.

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
then `scd`, `genie` after `metadata`, and `dashboard` after `quality`. Running `metadata` after the load keeps the time of the last change of the
tables (`last_altered`) for the loads that succeeded.

## Command line

`cds validate <root>` loads the data products of a root folder, validates their
contracts and configurations, including their tags, service levels, Genie spaces and
dashboards, and
validates the inferred checks with DQX, without Spark. `cds checks <root> --data-product <group>.<data_product>` prints the inferred
checks, `cds genie <root> --data-product <group>.<data_product>` the Genie space, and
`cds dashboard <root> --data-product <group>.<data_product>` the quality dashboard.

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
