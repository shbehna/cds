# CDS demo: the enterprise register of Québec

This project uses the [CDS framework](../cds-framework/README.md) as a library to build
a data product from the open data of the
[Registraire des entreprises du Québec](https://www.donneesquebec.ca/) (REQ).

The CSV files of the REQ are the raw data. The data product turns them into clean
tables, with names and types defined by data contracts, quality checks and history.

## Layout

```text
products/                                   root folder of the data products
└── donnees_qc/                             group
    ├── donnees_qc_config.py                naming conventions, contract defaults
    └── req/                                data product
        ├── req_config.py                   description, history settings, contract defaults
        ├── sources.py                      reading of the CSV files, shared by the contracts
        ├── domains.py                      checks of the codes against the value domains
        ├── entreprise/
        │   ├── entreprise.yaml             ODCS data contract
        │   └── entreprise_config.py        transformation and additional checks
        ├── etablissement/
        ├── nom_entreprise/
        ├── fusion_scission/
        ├── continuation_transformation/
        └── domaine_valeur/
dashboards/                                 quality dashboard, generated from the contracts
deploy/                                     deployment with Pulumi
tests/                                      tests of the data products and the dashboard, without Spark
```

## Language

The business domain is in French, the language of the register: the names of the
tables and columns, and the documentation of the contracts (descriptions, labels of the
codes, usage and limitations), which becomes the comments of the tables and columns in
Unity Catalog. Names are snake_case, without accents or abbreviations: `DAT_IMMAT`
becomes `date_immatriculation`, and the indicators (`IND_*`) become boolean columns
named `indicateur_*`. Each column description gives its source column.

The code, its comments and the technical documentation, such as this file, are in
English, as are the technical columns added by the framework (`_errors`, `_warnings`,
`_valid_from`...).

## Tables

The tables are published in the `<environment>.donnees_qc_req` schema, following the
naming convention of the `donnees_qc` group.

| Table | Source file | Primary key |
| --- | --- | --- |
| `entreprise` | `Entreprise.csv` | `numero_entreprise` |
| `etablissement` | `Etablissements.csv` | `numero_entreprise`, `numero_etablissement` |
| `nom_entreprise` | `Nom.csv` | `numero_entreprise`, `nom`, `code_type_nom`, `date_inscription` |
| `fusion_scission` | `FusionScissions.csv` | none: the register publishes no unique key |
| `continuation_transformation` | `ContinuationsTransformations.csv` | `numero_entreprise`, `code_type_changement`, `date_effet` |
| `domaine_valeur` | `DomaineValeur.csv` | `domaine`, `code` |

The tables with a primary key have a history table, `<table>_historique`: the
`donnees_qc` group overrides the `<table>_history` default of the framework.

## How the services use the contracts

- **Schema**: the contracts define the tables, their column types and comments.
- **Transformation**: the `transform` of each contract configuration reads its CSV file
  and renames its columns; indicators (`O`/`N`) become booleans. The service casts the
  result to the types of the contract and rejects any column it does not declare.
- **Quality**: the checks are inferred from the contracts: required columns, primary
  keys, foreign keys to `entreprise`, allowed values (`enum`), the format of the
  enterprise number, a minimum row count and a custom DQX rule. The contract
  configurations add checks computed at run time: the codes of activities and laws
  must exist in `domaine_valeur`.
- **SCD**: the history uses the primary key of each contract. The register publishes a
  few rows sharing the same key, which the quality service reports: the `req`
  configuration keeps one row per key in the history instead of failing.

A profiling of the data set of October 2026 shows what the checks catch: 25 keys of
establishments, 2,245 keys of names and 91 keys of continuations used by several rows,
16 enterprises without registration date, and a name and a merger referencing an
unknown enterprise.

## Quality dashboard

The Databricks AI/BI dashboard **REQ data quality** follows the results of the
quality service. It belongs to the demo, not to the framework: it reads the `_errors`
and `_warnings` columns that the quality service writes in each row.

- **Statistics**: a pie chart of the rows without issue, with warnings and with errors
  (a row with both is in error), a bar chart of the occurrences of each check, and a
  table of the occurrences by table, check and severity. Filter: table.
- **Occurrences**: each error and warning, with the primary key of its row as a JSON
  object (`{"numero_entreprise": "1140000016"}`), the check, its message and columns.
  `fusion_scission` has no primary key: its rows are identified by all their columns.
  The check time is shown in the time zone of the SQL warehouse, UTC by default.
  Filters: table, check, severity (error or warning).

[dashboards/quality_dashboard.py](dashboards/quality_dashboard.py) generates the
dashboard, [dashboards/req_quality.lvdash.json](dashboards/req_quality.lvdash.json),
from the contracts: their tables, primary keys and result columns. Its queries name
the tables without catalog nor schema: the deployment sets those of each environment.
After changing the contracts, regenerate it and commit it; a test checks that it is up
to date:

```bash
uv run python cds-demo/dashboards/quality_dashboard.py
```

## Run locally

From the root of the repository:

```bash
uv sync --all-packages
uv run cds validate cds-demo/products     # loads and validates the data products
uv run pytest cds-demo/tests              # tests the transformations and contracts
```

To run a service against a workspace with Databricks Connect, set the runtime
parameters as environment variables:

```bash
export CDS_ENVIRONMENT=nprod CDS_ROOTS=cds-demo/products CDS_DATA_PRODUCT=donnees_qc.req
export CDS_SOURCE_PATH=/Volumes/ext/donnees_qc/req
uv run python -c "from cds import SchemaService, run_service; run_service(SchemaService)"
```

## Deploy

See [deploy/README.md](deploy/README.md).
