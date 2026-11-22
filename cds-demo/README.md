# CDS demo: open data of Québec

This project uses the [CDS framework](../cds-framework/README.md) as a library to build
data products from the open data of [Données Québec](https://www.donneesquebec.ca/):

- `req`: the enterprise register of the Registraire des entreprises du Québec (REQ);
- `repertoire_municipalites`: the
  [directory of the municipalities](https://www.donneesquebec.ca/recherche/dataset/repertoire-des-municipalites-du-quebec)
  of the ministère des Affaires municipales et de l'Habitation (see
  [The directory of the municipalities](#the-directory-of-the-municipalities)).

The CSV files of the data sets are the raw data. The data products turn them into clean
tables, with names and types defined by data contracts, quality checks and history.
Unless stated otherwise, the sections below describe `req`.

## Layout

```text
products/                                   root folder of the data products
└── donnees_qc/                             group
    ├── donnees_qc_config.py                naming conventions, history table names, metadata
    ├── donnees_qc.odcs.yaml                contract defaults of the group: tenant, tags
    ├── sources.py                          reading of the CSV files, shared by the data products
    ├── req/                                data product
    │   ├── req_config.py                   quality and history settings, Genie space, quality dashboard
    │   ├── req.odps.yaml                   ODPS document: description, domain, status, support, usage
    │   ├── req.odcs.yaml                   contract defaults: service levels
    │   ├── sources.py                      conversions of the values, shared by the contracts
    │   ├── questions.py                    instructions and questions of the Genie space
    │   ├── entreprise/
    │   │   ├── entreprise.yaml             ODCS data contract
    │   │   └── entreprise_config.py        transformation and additional checks
    │   ├── etablissement/
    │   ├── nom_entreprise/
    │   ├── fusion_scission/
    │   ├── continuation_transformation/
    │   └── domaine_valeur/
    └── repertoire_municipalites/           data product
        ├── repertoire_municipalites_config.py  history settings, Genie space, quality dashboard
        ├── repertoire_municipalites.odps.yaml  ODPS document
        ├── repertoire_municipalites.odcs.yaml  contract defaults: service levels
        ├── sources.py                      conversions of the values and of the seats of the councils
        ├── questions.py                    instructions and questions of the Genie space
        ├── municipalite/
        ├── conseiller_municipal/
        ├── organisme_supramunicipal/
        ├── arrondissement/
        ├── conseiller_arrondissement/
        └── ressource/
csv/                                        synthetic data set and its generator
manifest/                                   what the deployment needs of the contracts, generated from them
deploy/                                     deployment with Pulumi
tests/                                      tests of the data products, the data set, the dashboards and the Genie spaces, without Spark
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
| `nom_entreprise` | `Nom.csv` | `numero_entreprise`, `nom`, `code_type_nom`, `date_inscription`, `nom_langue_etrangere` |
| `fusion_scission` | `FusionScissions.csv` | none: the register publishes no unique key |
| `continuation_transformation` | `ContinuationsTransformations.csv` | `numero_entreprise`, `code_type_changement`, `date_effet`, `code_regime_juridique` |
| `domaine_valeur` | `DomaineValeur.csv` | `domaine`, `code` |

The tables with a primary key have a history table, `<table>_historique`: the
`donnees_qc` group overrides the `<table>_history` default of the framework.

## How the services use the contracts

- **Schema**: the contracts define the tables, their column types and comments.
- **Metadata**: the descriptions of the contracts become the comments of the tables and
  columns, and their tags Unity Catalog tags: `donnees_quebec` and `donnees_ouvertes`,
  set by the `donnees_qc` group, `domain` and `data_product`. The tables are tagged with
  their Discover domain (see [Discover domains](#discover-domains)). The `donnees_qc` group
  certifies the tables by the status of their contract: Catalog Explorer shows the
  tables of the active contracts as certified, and `fusion_scission`, whose contract is
  deprecated for the demonstration, as deprecated. The classification of the columns
  becomes their `classification` tag, on the tables and their history tables (see
  [Classification and masking](#classification-and-masking)).
- **Transformation**: the `transform` of each contract configuration reads its CSV file
  and renames its columns; indicators (`O`/`N`) become booleans. The service casts the
  result to the types of the contract and rejects any column it does not declare.
- **Quality**: the checks are inferred from the contracts: required columns, primary
  keys, foreign keys to `entreprise`, allowed values (`enum`), the format of the
  enterprise number and of the value domains, and custom DQX rules, as warnings, on the
  consistency of the dates and of the relations. The codes of activities and laws must
  exist in their value domain of `domaine_valeur`: their `allowedValues` custom property
  names it, and the quality service reads its codes at run time. The register uses
  codes missing from its value domains: the `req` configuration reports them as
  warnings. The configuration of `entreprise` adds a check: it must have at least a
  million rows (a hundred with the synthetic data set).
- **SCD**: the history uses the primary key of each contract. The register publishes a
  few rows sharing the same key, which the quality service reports: the `req`
  configuration keeps one row per key in the history instead of failing. The history
  keeps the replaced versions 10 years (see [Service levels](#service-levels)).

A profiling of the data set of October 2026 shows what the checks catch: 25 keys of
establishments and 2,245 keys of names used by several rows,
16 enterprises without registration date, and a name and a merger referencing an
unknown enterprise.

## Classification and masking

The contracts classify the sensitive columns with the ODCS `classification` of their
properties. The `donnees_qc` group declares the allowed classifications, which `cds
validate` checks: `public`, `internal`, `confidential` and `restricted`. The addresses are
`restricted` (`adresse_ligne_1` to `adresse_ligne_4`): those of the establishments
(`etablissement`), of the home of the enterprises (`entreprise`) and of the linked
enterprises (`fusion_scission`).

The services tag each classified column, and the same column of the history table, with
its classification: `classification = restricted`. The schema and scd services tag the
columns of the tables they create before writing to them, and the metadata service keeps
the tags in sync. The deployment declares `classification` as a governed tag, and an ABAC policy of
the catalog masks the columns tagged `restricted` for every user except the service
principal of the job (see
[deploy/README.md](deploy/README.md#masking-of-the-classified-columns)). Classifying
another `STRING` column in a contract is enough to mask it: the policy matches the tags,
not the columns. The masking function only takes strings, which a test checks.

The deployment cannot load the data products: it reads the classifications in the
manifest [manifest/manifest.json](manifest/manifest.json), generated from the
contracts by [manifest/manifest.py](manifest/manifest.py), with the Discover domains,
and for each data product the schedule of its job and the alerts of its service levels.
The data products share the classifications: the masking policy applies to the whole
catalog. Regenerate it after changing the contracts or their
configurations, and commit it: a test checks that it is up to date.

```bash
uv run python cds-demo/manifest/manifest.py
```

## Discover domains

The [Discover page](https://docs.databricks.com/aws/en/discover/discover-page) of
Databricks presents the assets by business domain. The tables of the data product are
in the subdomain **Registraire des entreprises du Québec** of the domain **Économie**:

- the ODPS document of `req` (`req.odps.yaml`) sets the `domain` of its contracts to
  `Économie`, and their `subdomain` custom property to
  `Registraire des entreprises du Québec`;
- the `donnees_qc` group makes them the Discover domain of the tables
  (`MetadataSettings.discover_domain`): the metadata service tags each table with the
  governed tag of the subdomain, `Économie/Registraire des entreprises du Québec`;
- in `prod` only (`MetadataSettings.discover_environments`): the Discover page shows the
  data of production, not its `nprod` copy.

The tables then appear on the page of the subdomain, and on that of the domain. They also
keep the `domain` tag, `domain = Économie`. The history tables are not in the domain:
their contract is that of their table. The [quality dashboard](#quality-dashboard) and the
[Genie space](#genie-space) of `prod` join the subdomain of their tables: the
`dashboard` and `genie` services tag them with the same governed tag.

A Discover domain is built on its governed tag, and its name is the key of the tag. The
deployment creates the governed tags and the domains, which the manifest lists, parents
first (see [deploy/README.md](deploy/README.md#discover-domains)).

## Service levels

The partial data contract of `req` (`req.odcs.yaml`) gives every contract the service
levels of the data product (`slaProperties`), and its ODPS document (`req.odps.yaml`) a
support channel:

| Property | Value | Checked | Applied by |
| --- | --- | --- | --- |
| `frequency` | the 1st and the 15th of each month, at 6 am (`0 6 1,15 * *`): the register is published twice a month | | the schedule of the job |
| `latency` | 18 days since the last change of each table: up to 17 days separate two loads | every day at 7 am | an alert |
| `errorRate` | at most 1 % of the rows of each table with a quality error | after each load, the 1st and the 15th at noon | an alert |
| `retention` | the history keeps the replaced versions 10 years | | the scd service |

The manifest converts them for the deployment: the schedule of the job, and the queries
of the two SQL alerts (see
[deploy/README.md](deploy/README.md#alerts-of-the-service-levels)). Each query returns a
row per table: its error rate, or the time of its last change, and whether it breaks its
service level. The alerts are sent to the email support channel of the contracts, whose
address is fictitious, and to the addresses of the deployment. The 5 % of rows in error
of the synthetic data set trigger the error rate alert in `nprod`.

The latency measures the last change of a table (`last_altered` of the information
schema), which the metadata step updates after each load. It does not run when the load
fails: the alert detects the failed loads and a job that no longer runs. A contract can
measure the freshness of its data instead, with a date column as the `element` of its
latency: the alert then checks the latest value of the column. The failed runs of the job
also send an email.

## Synthetic data set

[csv/testdata](csv/testdata) holds a synthetic version of each file of the data sets,
named after it (`Entreprise_testdata.csv` for `Entreprise.csv`), in the folder of its
data product (`donnees_qc/req`, `donnees_qc/repertoire_municipalites`), to illustrate the
quality service. Each file has 200 rows: 170 valid rows (85 %), 20 rows with a warning
(10 %) and 10 rows with an error (5 %). Each of these rows fails a single check, chosen
among the kinds of checks of its table:

| Table | Errors | Warnings |
| --- | --- | --- |
| `entreprise` | enterprise number not matching its pattern, codes out of their `enum`, missing registration date, duplicated key | codes out of their value domain, limited liability ending before it starts |
| `etablissement` | unknown enterprise, establishment number 0 or missing, invalid indicator, duplicated key | activity codes out of their value domain |
| `nom_entreprise` | unknown enterprise, missing name, codes out of their `enum`, duplicated key | name withdrawn before its registration, former name without withdrawal date |
| `fusion_scission` | unknown enterprise or linked enterprise, code out of its `enum`, missing date | enterprise linked to itself, date in the future |
| `continuation_transformation` | unknown or missing enterprise, code out of its `enum`, missing date, duplicated key | law code out of its value domain, date in the future |
| `domaine_valeur` | missing domain, code or description, duplicated key | code with spaces, domain not in capitals |
| `municipalite` | code not matching its pattern, designation out of its `enum`, invalid postal code or phone number, negative population, duplicated key | land area larger than the total area, phone number or election date not filled in |
| `organisme_supramunicipal` | code not matching its pattern, designation out of its `enum`, invalid postal code or fax number, negative population, duplicated key | land area larger than the total area, phone number not filled in |
| `arrondissement` | unknown municipality, invalid postal code or phone number, missing name, duplicated key | phone number or election date not filled in |
| `ressource` | group out of its `enum`, missing title code or organisation, invalid indicator, duplicated key | position held without holder, entry date not filled in |

The other rows reference each other as in the published files: the establishments,
names, mergers and continuations belong to enterprises without errors, and their codes
belong to the value domains of `DomaineValeur_testdata.csv`; the boroughs belong to
municipalities without errors. The seats of the councils (`conseiller_municipal` and
`conseiller_arrondissement`) are all valid: their code is that of their municipality or
borough, and the municipalities and boroughs with errors have no councillors. The codes,
names, addresses and phone numbers of the directory are fictitious: its municipalities
are numbered from `00001`, and their addresses use the `example` domain.

The transformations read the synthetic files instead of the published ones when the
runtime parameter `synthetic_data` is true ([sources.py](products/donnees_qc/sources.py)).
The deployment uploads them next to the published files, and the `nprod` jobs load them;
the `prod` jobs load the published files. A data product without synthetic data set
loads its published files in `nprod` too.

[csv/generate_testdata.py](csv/generate_testdata.py) generates the files,
deterministically. Regenerate them after changing it, and commit them: a test checks
that they are up to date, and that each row fails exactly the checks the generator
expects.

```bash
uv run python cds-demo/csv/generate_testdata.py
```

## Quality dashboard

The Databricks AI/BI dashboard **REQ data quality (&lt;environment&gt;)** follows the
results of the quality service. The framework generates it from the contracts (see
[cds.dashboard](../cds-framework/README.md#quality-dashboards)): their tables, primary keys
and result columns. The `dashboard` settings of
[req_config.py](products/donnees_qc/req/req_config.py) enable it and give its title.

- **Statistics**: a pie chart of the rows without issue, with warnings and with errors
  (a row with both is in error), a bar chart of the occurrences of each check, and a
  table of the occurrences by table, check and severity. Filter: table.
- **Occurrences**: each error and warning, with the primary key of its row as a JSON
  object (`{"numero_entreprise": "1140000016"}`), the check, its message and columns.
  `fusion_scission` has no primary key: its rows are identified by all their columns,
  except the restricted addresses. The check time is shown in the time zone of the SQL
  warehouse, UTC by default. Filters: table, check, severity (error or warning).

The `dashboard` step of the job creates and publishes it, and updates it on each run when
the contracts change (see [deploy/README.md](deploy/README.md#quality-dashboard)). To see
it without deploying it:

```bash
uv run cds dashboard cds-demo/products --data-product donnees_qc.req --environment nprod
```

## Genie space

A [Genie space](https://docs.databricks.com/aws/en/genie/) answers questions on the
tables of the data product in natural language, by writing SQL. The framework generates
the space of `req` from its contracts and its ODPS document (see
[cds.genie](../cds-framework/README.md#genie-spaces)): its tables, the joins of their
foreign keys (`fusion_scission` joins `entreprise` twice, the second time as
`entreprise_liee`), the labels of the `enum` codes, and instructions on the keys, the
deprecated `fusion_scission`, the quality results, the error rate, the masked addresses
and the frequency of the loads.

The `genie` settings of [req_config.py](products/donnees_qc/req/req_config.py) enable it,
and add, from [questions.py](products/donnees_qc/req/questions.py), what the contracts do
not say, in French:

- how to describe the codes without `enum`, such as the economic activities: join
  `domaine_valeur` on the value domain named in the description of the column;
- sample questions, and two examples with their SQL: the most frequent economic
  activities, and the error rate of each table, generated by `cds.genie.error_rate_sql`;
- the masked classification, `restricted`, which the deployment masks
  (`cds-demo:maskedClassifications`).

The `genie` step of the job creates the space, **Registre des entreprises du Québec
(&lt;environment&gt;)**, and updates it on each run when the contracts change (see
[deploy/README.md](deploy/README.md#genie-space)). Its queries run with the permissions of
each user: the addresses are masked. To see the space without deploying it:

```bash
uv run cds genie cds-demo/products --data-product donnees_qc.req --environment nprod
```

## The directory of the municipalities

The data product `repertoire_municipalites` is built from the four CSV files of the
[Répertoire des municipalités du Québec](https://www.donneesquebec.ca/recherche/dataset/repertoire-des-municipalites-du-quebec),
published under the CC BY 4.0 license. It is in the subdomain **Ministère des Affaires
municipales et de l'Habitation** of the Discover domain **Gouvernement**. Its tables are
published in the `<environment>.donnees_qc_repertoire_municipalites` schema.

| Table | Source file | Primary key |
| --- | --- | --- |
| `municipalite` | `MUN.csv` | `code_geographique` |
| `conseiller_municipal` | `MUN.csv`, columns `con1` to `con75` | `code_geographique`, `numero_siege` |
| `organisme_supramunicipal` | `MRC_CM_Arg.csv`: the MRC, the metropolitan communities and the Kativik Regional Government | `code_geographique` |
| `arrondissement` | `ARR.csv` | `code_geographique` |
| `conseiller_arrondissement` | `ARR.csv`, columns `con01` to `con75` | `code_geographique`, `numero_siege` |
| `ressource` | `ress.csv`: the elected officials and the officers, one row per position | `code_organisation`, `code_titre` |

The contracts and their transformations follow those of `req`, with these differences:

- **Values**: every value of the files is quoted, so Spark reads the empty ones as empty
  strings. The transformations trim the values and turn the empty ones into nulls, which
  the casts to dates and numbers need. The indicators (`oui`/`non`) become booleans.
- **Councils**: `MUN.csv` and `ARR.csv` have a column per seat of the council, mostly
  empty. The seats become the rows of `conseiller_municipal` and
  `conseiller_arrondissement`, one per seat with a councillor. The number of the seat is
  that of its column. A vacant seat (`Poste vacant`) has a row without name, with
  `indicateur_poste_vacant`.
- **Unused column**: `nd` of `MUN.csv`, which the publisher does not use, is not loaded.
- **Classification**: the names of persons (mayors, councillors, officers) are
  `confidential`. They are tagged, but not masked: the deployment only masks `restricted`.
- **Quality**: primary keys, foreign keys to `municipalite` and `arrondissement`, the
  formats of the codes, postal codes and phone numbers, and the codes of the designations
  (`enum`). Warnings report the values the directory uses for unknown values: the phone
  number `0000000000`, the election date `0001-01-01` and the entry dates of 1900. The
  configuration of `municipalite` adds a check: it must have at least a thousand rows.
  On the files of November 2026, the checks find no error, 155 phone numbers and 2
  election dates not filled in, and 131 positions without entry date.

`ressource` has no foreign key: its positions belong to municipalities, supra-municipal
bodies, boroughs or agglomerations (`AG` codes), which the files do not describe.

The other services cover it as they cover `req`:

- **Deployment**: its job, `cds-demo-repertoire_municipalites-<environment>`, reads its
  files in `/Volumes/ext/donnees_qc/repertoire_municipalites` (see
  [deploy/README.md](deploy/README.md)).
- **Service levels**: its partial data contract (`repertoire_municipalites.odcs.yaml`)
  loads the tables every day at 5 am, as the directory is updated every day. The latency
  alert checks every day at 8 am that they changed in the last 2 days, and the error rate
  alert every day at 9 am that at most 1 % of their rows have an error. The history keeps
  the replaced versions 10 years. In `prod`, the tables are as recent as the files of its
  volume, uploaded by hand.
- **Synthetic data set**: see [Synthetic data set](#synthetic-data-set). Its rows sharing
  a key are reported by the quality service; the history keeps one row per key.
- **Quality dashboard**: **Répertoire des municipalités data quality
  (&lt;environment&gt;)**.
- **Genie space**: **Répertoire des municipalités du Québec (&lt;environment&gt;)**, with
  the joins of the foreign keys, and instructions in French
  ([questions.py](products/donnees_qc/repertoire_municipalites/questions.py)) on the
  vacant seats, the organisations of the positions, the codes in parentheses and the
  unknown values. The `confidential` names are not masked.

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
export CDS_SOURCE_PATH=/Volumes/ext/donnees_qc/req CDS_SYNTHETIC_DATA=true   # false: the register
uv run python -c "from cds import SchemaService, run_service; run_service(SchemaService)"
```

## Deploy

See [deploy/README.md](deploy/README.md).
