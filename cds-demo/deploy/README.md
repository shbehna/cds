# Deployment of the demo

The demo is deployed to Databricks by [Pulumi](https://www.pulumi.com/), run by a
GitHub Actions workflow. This guide sets everything up, step by step.

## What is deployed

The Pulumi program ([pipeline.py](pipeline.py)) creates, in the workspace folder
`/Shared/cds-demo/<environment>`:

```text
/Shared/cds-demo/nprod/
├── alerts/          the SQL alerts of the service levels (cds-demo/manifest)
├── dashboards/      the quality dashboards of the data products, created by their jobs
├── genie/           the Genie spaces of the data products, created by their jobs
├── notebooks/       the service notebooks of the framework (cds-framework/notebooks)
├── products/        the data products of the demo (cds-demo/products)
└── wheels/<hash>/   the wheel of the framework, built by the workflow
```

It also creates a job for each data product of the manifest, which runs the services of
the framework on serverless compute, on the schedule of the `frequency` of its contracts
(`cds-demo:timezone`):

| Data product | Job | Schedule | Files |
| --- | --- | --- | --- |
| `donnees_qc.req` | `cds-demo-req-<environment>` | the 1st and the 15th of each month at 6 am | `/Volumes/ext/donnees_qc/req` |
| `donnees_qc.repertoire_municipalites` | `cds-demo-repertoire_municipalites-<environment>` | every day at 5 am | `/Volumes/ext/donnees_qc/repertoire_municipalites` |

Each job reads the files of its data product in its folder under `cds-demo:sourceRoot`:
`<sourceRoot>/<group>/<data product>`. The schedules are paused unless
`cds-demo:pauseSchedule` is `false`, as in `prod`: in `nprod`, the jobs run after each
deployment, and on demand.
The sections below describe the job of `req`: the job of another data product runs the
same steps on its own tables, in its own schema.

With `cds-demo:syntheticData`, the jobs of the data products of the synthetic data set
([cds-demo/csv/testdata](../csv/testdata), a folder `<group>/<data product>` each) load
it; the others load their published files, as in `prod`.

The resources of a data product are named after it. The first deployment of this
version renames the job, the alerts and the synthetic files of `req` (`pipeline` becomes
`pipeline:donnees_qc.req`): their former names are Pulumi aliases, so that Pulumi renames
them instead of replacing them.

```text
load --> metadata --> publication
     --> quality  --> publication
                  --> scd
```

The job has 5 tasks, each a notebook of the framework: Databricks Free Edition refuses a
run of a job of more than 5 tasks, with the error "There are already 5 active runs
(limit: 5)", even when nothing else runs. The `load` task runs the schema step, then the
transformation step; the `publication` task runs the genie step, once the metadata step
has set the comments of the tables, then the dashboard step, once the quality step has
added its result columns.

A failed step fails the run, and the steps after it do not run, in its task or in the
tasks after it: a failed genie step leaves the dashboard as it was. A failed run sends an
email to the addresses of the alerts (see
[Alerts of the service levels](#alerts-of-the-service-levels)). The metadata step runs
after the load: as it changes the tables on each run, a failed load leaves them
unchanged, which the latency alert detects.

### Masking of the classified columns

The contracts classify their columns (`classification`): the addresses of `entreprise`,
`etablissement` and `fusion_scission` are `restricted` (see
[cds-demo/README.md](../README.md#classification-and-masking)), and the names of the
persons of the directory of the municipalities `confidential`, which is not masked. The job tags each column
with its classification, before writing to it: the schema step tags the columns of the
tables it creates, and the scd step those of the history tables. With `cds-demo:masking`, the deployment masks the
columns of the classifications `cds-demo:maskedClassifications` (`restricted`):

| Resource | Name | Role |
| --- | --- | --- |
| Governed tag | `classification` | The allowed values of the classifications, from [cds-demo/manifest](../manifest). It belongs to the account: only the `nprod` stack creates it (`createTagPolicies`), protected (see [Deploy from your computer](#deploy-from-your-computer-optional)). Unity Catalog takes a while to know a new governed tag: the deployment waits 2 minutes after creating it, before creating the policy. |
| Schema | `<environment>.governance` | Holds the masking function. |
| SQL function | `<environment>.governance.mask` | Replaces a string by `****`. The provider has no resource for SQL functions: the deployment runs `CREATE OR REPLACE FUNCTION` on the SQL warehouse ([sql_statement.py](sql_statement.py)). |
| ABAC policy | `mask_restricted`, on the catalog `<environment>` | Masks the columns tagged `classification = restricted` of every table of the catalog, history tables included, for every user of the account except the service principal and `cds-demo:maskingExemptPrincipals`. |

The service principal is exempted: it runs the job, whose quality checks and history need
the data as it is. The dashboard runs its queries with its credentials: it leaves the
classified columns out of the key of the rows it shows.

The masking function takes and returns a string: only classify as `restricted` the columns of
type `STRING`, which a test of the demo checks (`cds-demo/tests/test_manifest.py`).

The deployment reads the classifications in the manifest
[cds-demo/manifest/manifest.json](../manifest/manifest.json), generated from the
contracts: regenerate it after changing them (`uv run python cds-demo/manifest/manifest.py`).
The data products share the classifications: the masking policy applies to the whole
catalog.

### Discover domains

In `prod`, the metadata step of the jobs tags each table with the governed tag of its
Discover domain: the subdomain **Registraire des entreprises du Québec** of the domain
**Économie** for `req`, the subdomain **Ministère des Affaires municipales et de
l'Habitation** of the domain **Gouvernement** for `repertoire_municipalites` (see
[cds-demo/README.md](../README.md#discover-domains)). The `genie` and
`dashboard` steps tag the Genie space and the quality dashboard with it: the subdomain
lists them with the tables. The `nprod` job does not, and removes the tags it applied
before. With `cds-demo:createDomains`, the deployment creates them, from the manifest:

| Resource | Name | Role |
| --- | --- | --- |
| Governed tags | `Économie`, `Économie/Registraire des entreprises du Québec`, `Gouvernement`, `Gouvernement/Ministère des Affaires municipales et de l'Habitation` | Tags without values. The tag of a subdomain is its path under its domain. As for `classification`, the deployment waits 2 minutes after creating them, before creating the domains and the job that applies them. |
| Discover domains | **Économie** and **Gouvernement**, and their subdomains | Built on the governed tags, and published. Their subtitles and descriptions, in French, are in `DOMAINS` ([pipeline.py](pipeline.py)): a domain of the contracts without a description fails the deployment. |

The governed tags and the domains belong to the account: only the `nprod` stack creates
them, protected, and the `prod` job applies them. Only the `prod` assets are in the
domains (`discover_environments` in
[donnees_qc_config.py](../products/donnees_qc/donnees_qc_config.py)): the Discover page
does not show their `nprod` copies.

Renaming a domain or a subdomain in the contracts replaces its governed tag and its
domain. As they are protected, the `nprod` deployment then fails to delete the old ones:
once the `prod` job has tagged its tables with the new tag, run `pulumi state unprotect`
on the old tag and domain of the `nprod` stack, and deploy again.

### Quality dashboard

The `dashboard` step of the job creates the quality dashboard of the data product, **REQ
data quality (&lt;environment&gt;)**, and **Répertoire des municipalités data quality
(&lt;environment&gt;)** for `repertoire_municipalites`, in the `dashboards` folder, from the contracts (see
[cds-demo/README.md](../README.md#quality-dashboard)), and publishes it. The next runs
update it, and publish it again, when the contracts change. The job keeps its id in the
`cds.dashboard_id` property of the schema of its data product (`donnees_qc_req`). It receives two parameters
from the deployment: the SQL warehouse (`dashboard_warehouse_id`) and the folder
(`dashboard_parent_path`). The deployment does not manage the dashboard: deleting the
`dashboards` folder, as `pulumi destroy` does, deletes it.

The dashboard is published with the credentials of the service principal, which owns the
tables and which the masking exempts: its viewers need no access to the tables, and it
leaves the restricted addresses out of the keys of the rows it shows.

The first deployment of this version deletes the dashboard of the previous versions,
which the deployment published: the next run of the job creates the new one, at a new URL.

### Genie space

The `genie` step of the job creates the Genie space of the data product, **Registre des
entreprises du Québec (&lt;environment&gt;)**, and **Répertoire des municipalités du Québec
(&lt;environment&gt;)** for `repertoire_municipalites`, in the `genie` folder, from the contracts
(see [cds-demo/README.md](../README.md#genie-space)). The next runs update it when the
contracts change. The job keeps its id in the `cds.genie_space_id` property of the schema
of its data product (`donnees_qc_req`). It receives two parameters from the deployment: the SQL warehouse of the
dashboard (`genie_warehouse_id`), and the folder (`genie_parent_path`).

| Resource | Name | Role |
| --- | --- | --- |
| Folder | `<workspaceRoot>/genie` | Holds the space, created by the job: the deployment does not manage it. Deleting the folder, as `pulumi destroy` does, deletes the space. |
| Permissions | `CAN RUN` on the folder | The principals of `cds-demo:genieUsers` (`users`, every user of the workspace) can view the space and ask questions: the space inherits the permissions of its folder. |

The folder also inherits the permissions of its parents: under `/Shared`, the default
workspace root, the `users` group already has `CAN MANAGE` on it, and so on the space. To
restrict the space to `CAN RUN`, set a `cds-demo:workspaceRoot` outside `/Shared`.

The space runs its queries with the permissions of the user asking, on the SQL warehouse of
the dashboard: users need `CAN USE` on the warehouse, which the `users` group has on the
Serverless Starter Warehouse, and `SELECT` on the tables. The deployment does not grant
them: the permissions of a warehouse are shared with its other uses. Users see the
addresses masked (`****`). The service principal owns the space.

### Alerts of the service levels

The contracts state service levels (see
[cds-demo/README.md](../README.md#service-levels)), which the manifest turns into two SQL
alerts for each data product, in the `alerts` folder:

| Alert | Triggers when | Checked |
| --- | --- | --- |
| **donnees_qc.req error rate SLA (&lt;environment&gt;)** | more than 1 % of the rows of a table have a quality error | after each load: the 1st and the 15th of each month at noon |
| **donnees_qc.req latency SLA (&lt;environment&gt;)** | a table has not changed for 18 days, as its load failed or the job did not run | every day at 7 am |
| **donnees_qc.repertoire_municipalites error rate SLA (&lt;environment&gt;)** | more than 1 % of the rows of a table have a quality error | after each load: every day at 9 am |
| **donnees_qc.repertoire_municipalites latency SLA (&lt;environment&gt;)** | a table has not changed for 2 days | every day at 8 am |

Their queries run on the SQL warehouse of the dashboard, as the service principal, and
return a row per table: the alert triggers when one of them breaks its service level. It
then sends an email to the notification destination of its data product, **CDS demo
alerts, &lt;data product&gt; (&lt;environment&gt;)**, holding the address of the support
channel of its contracts (fictitious) and those of the GitHub secret `ALERT_EMAILS`, and another one when it is back to
normal. Creating a notification destination requires a workspace administrator, which the
service principal is.

The error rate is checked once the job has loaded and checked the tables. If the quality
step fails, the error rate of the tables cannot be measured: the failed run sends its own
email.

In `nprod`, run the error rate alert after a run of the job (**Run now** on the alert):
the error rate of the synthetic data set triggers it. As the schedules of the jobs are
paused, the latency alerts trigger 18 days (`req`) and 2 days
(`repertoire_municipalites`) after the last run.

To receive the alerts, add your address to the GitHub secret `ALERT_EMAILS` (see
[Add the secrets to GitHub](#3-add-the-secrets-to-github-once)): the next deployment adds
it to the notification destinations of both environments. The deployment reads it as a
Pulumi secret, kept encrypted in the state of the stacks and hidden in their logs.

The jobs run as the deployment service principal. The tables are created in the
`<environment>` catalog, in the schema of their data product: `donnees_qc_req` and
`donnees_qc_repertoire_municipalites`.

The two environments differ by their data and their catalog:

| | `nprod` | `prod` |
| --- | --- | --- |
| Data loaded by the jobs | the synthetic data set ([cds-demo/csv/testdata](../csv/testdata)), uploaded by the deployment next to the published files | the published files |
| Catalog | created by an administrator | created by an administrator with Default Storage, owned by the service principal |

The synthetic data set illustrates the quality service: 85 % of valid rows, 10 % with
warnings and 5 % with errors (see [cds-demo/README.md](../README.md#synthetic-data-set)).
The jobs receive the choice as their `synthetic_data` parameter.

Pulumi keeps track of what it deployed in a *state*, stored by Pulumi Cloud, and
updates the workspace to match the program on each deployment. Each environment is a
Pulumi *stack*, `nprod` or `prod`, configured by [Pulumi.nprod.yaml](Pulumi.nprod.yaml) and
[Pulumi.prod.yaml](Pulumi.prod.yaml).

## How the workflow runs

The workflow [.github/workflows/deploy-demo.yml](../../.github/workflows/deploy-demo.yml):

runs on each push to `master` that changes the framework or the demo, and when you run
it manually. It chains three jobs, each starting only if the previous one succeeded:

```text
build --> deploy nprod --> deploy prod
```

- **Build** runs the tests of the repository and of the deployment program, and builds
  the wheel of the framework.
- **Deploy nprod** runs `pulumi up` on the `nprod` stack, with that wheel, then runs the
  jobs of the data products of `nprod` and waits for their result.
- **Deploy prod** does the same on the `prod` stack, once `nprod` is deployed and its
  jobs succeeded.

The jobs run one after the other, as Databricks Free Edition refuses a run while 5 tasks
are active ([run_jobs.py](run_jobs.py)). Each run has 10 minutes to finish
(`PIPELINE_TIMEOUT_MINUTES` in the workflow): a run still going is then canceled. A
failed, skipped or canceled run fails the deployment, whose logs show the failed tasks
and their message, after the other jobs have run. The script reads the jobs, the workspace
and the service principal in the outputs of the stack: `job_ids`, `databricks_host` and
`service_principal`.

Until the secrets below are configured, the build runs and the deployments are skipped
with a warning instead of failing.

## 1. Prepare Databricks (once)

### Upload the data

Create a volume per data product in the `ext` catalog, named after it:

```sql
CREATE SCHEMA IF NOT EXISTS ext.donnees_qc;
CREATE VOLUME IF NOT EXISTS ext.donnees_qc.req;
CREATE VOLUME IF NOT EXISTS ext.donnees_qc.repertoire_municipalites;
```

Then, in **Catalog > ext > donnees_qc > &lt;volume&gt;**, select **Upload to this volume**
and upload the files of its data set:

| Volume | Files | Source |
| --- | --- | --- |
| `req` | `Entreprise.csv`, `Etablissements.csv`, `Nom.csv`, `FusionScissions.csv`, `ContinuationsTransformations.csv`, `DomaineValeur.csv` | [Registraire des entreprises](https://www.donneesquebec.ca/) |
| `repertoire_municipalites` | `MUN.csv`, `MRC_CM_Arg.csv`, `ARR.csv`, `ress.csv` | [Répertoire des municipalités](https://www.donneesquebec.ca/recherche/dataset/repertoire-des-municipalites-du-quebec) |

The `prod` jobs load these files: upload them again to load a newer version. The
directory of the municipalities is updated every day, and its job loads it every day: the
tables are as recent as the files of the volume. If you use other volumes or folders, set
`cds-demo:sourceRoot` in both stack files: the folder of each data product is
`<sourceRoot>/<group>/<data product>`.

The `nprod` deployment uploads the synthetic files (`*_testdata.csv`) to the same volumes:
you do not upload them.

### Create a secret for the service principal

1. In the workspace, open **Settings > Identity and access > Service principals**.
2. Open the service principal `bf175dc4-c86b-4e88-83e9-9194c485a080`.
3. In **Secrets**, select **Generate secret** and copy the secret. It is shown only once.

### Grant access to the service principal

The account uses the Default Storage of Databricks: the metastore has no storage root,
and the API cannot create a catalog without a storage location. Create the `prod`
catalog in **Catalog > + > Create catalog**, with **Default Storage**. Then run, as a
metastore administrator:

```sql
-- nprod: the catalog is created by an administrator.
CREATE CATALOG IF NOT EXISTS nprod;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG nprod TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
-- prod: the catalog created in Catalog Explorer is given to the service principal.
ALTER CATALOG prod OWNER TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
-- The published files, and the synthetic files uploaded by the nprod deployment.
GRANT USE CATALOG ON CATALOG ext TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
GRANT USE SCHEMA, READ VOLUME ON SCHEMA ext.donnees_qc TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
GRANT WRITE VOLUME ON VOLUME ext.donnees_qc.req TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
GRANT WRITE VOLUME ON VOLUME ext.donnees_qc.repertoire_municipalites TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
```

The service principal owns the `prod` catalog, and the schemas and tables it creates.
As a metastore administrator or the owner of the `nprod` catalog, you can query them. To
let other users read the tables, grant them `USE CATALOG` on the catalog, and `USE
SCHEMA` and `SELECT` on the schemas of the data products: `donnees_qc_req` and
`donnees_qc_repertoire_municipalites`.

#### Creating the catalog with Pulumi

With a metastore storage root, or an external location for the catalog, the deployment
can create the catalog instead: set `cds-demo:createCatalog` to `"true"`, and
`cds-demo:catalogStorageRoot` to the location when the metastore has none (the service
principal then needs `CREATE MANAGED STORAGE` on the external location). Grant the
service principal `CREATE CATALOG ON METASTORE`; `cds-demo:catalogReaders` lists the
users or groups granted `USE CATALOG`, `USE SCHEMA` and `SELECT` on it. A catalog that
already exists is imported in the state of the stack, once, before the deployment:

```bash
pulumi import --stack prod databricks:index/catalog:Catalog catalog prod
```

### Let the service principal certify the tables

The metadata step certifies the tables with the governed tag `system.certification_status`
(see [cds-demo/README.md](../README.md#how-the-services-use-the-contracts)). In
**Catalog > Govern > Governed tags**, open `system.certification_status`, then
**Permissions**, and give the service principal **ASSIGN**. Without it, the metadata step
fails on the tables to certify.

### Let the service principal mask the classified columns

The masking ([What is deployed](#masking-of-the-classified-columns)) uses attribute-based
access control (ABAC) and governed tags. If the workspace shows them as previews, enable
them in **Settings > Previews** (*Attribute Based Access Control* and *Tag policies*).
Then, as a metastore administrator or the owner of the `nprod` catalog:

```sql
-- The masking policy is defined on the catalog. In prod, the service principal owns it.
GRANT MANAGE ON CATALOG nprod TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
```

The `nprod` deployment creates the governed tag `classification`: the service principal,
a workspace administrator, can create governed tags unless an account administrator
prevents it. It can then assign the tag, which the job does.

If the first deployment still fails with `Unknown tag policy key` on `classification`, the
governed tag has not propagated yet: run the deployment again a few minutes later.

### Let the service principal create the Discover domains

The [Discover domains](#discover-domains) need the **Discover page** preview: if the
workspace shows it as a preview, enable it in **Settings > Previews**. Creating domains
requires the `MANAGE DISCOVER` permission, and creating governed tags the `CREATE`
permission on governed tags: workspace administrators, such as the service principal,
have both, unless an account administrator revokes them. The service principal can then
assign the governed tags it creates, which the job does, to the tables, the Genie space
and the dashboard.

### Let the service principal use a SQL warehouse

The quality dashboard runs its queries on the SQL warehouse named by
`cds-demo:warehouseName` in the stack files (**placeholder**: `Serverless Starter
Warehouse`, the warehouse of a new workspace). Open **SQL Warehouses**, select the
warehouse, then **Permissions**, and give the service principal **Can use**. The
deployment fails if the service principal cannot see the warehouse.

Users viewing the published dashboard do not need access to the tables: its queries
run with the credentials of the service principal.

## 2. Create a Pulumi Cloud account (once)

1. Sign up at [app.pulumi.com](https://app.pulumi.com). The individual tier is free.
2. Open your avatar menu > **Personal access tokens** > **Create token**, and copy the token.

The stacks `nprod` and `prod` are created in your account on the first deployment.

## 3. Add the secrets to GitHub (once)

This is where the secret of the service principal goes: in GitHub, encrypted, and
never in the repository.

1. In the repository on GitHub, open **Settings > Secrets and variables > Actions**.
2. Select **New repository secret** and add:

   | Name | Value |
   | --- | --- |
   | `DATABRICKS_CLIENT_SECRET` | The secret of the service principal (step 1). |
   | `PULUMI_ACCESS_TOKEN` | The Pulumi access token (step 2). |
   | `ALERT_EMAILS` | Optional. The addresses receiving the alerts of the service levels, separated by commas, such as `you@example.com, teammate@example.com`. |

The workflow passes `DATABRICKS_CLIENT_SECRET` and `ALERT_EMAILS` to Pulumi as
environment variables. The deployments run without `ALERT_EMAILS`: the alerts are then
only sent to the fictitious address of the contracts.
The client id of the service principal is not secret: it is in the stack files.

GitHub masks secrets in the logs. If you later want an approval before deploying `prod`,
create a `prod` environment in **Settings > Environments** with required reviewers,
and add `environment: prod` to the `deploy-prod` job of the workflow: the run then waits
for the approval once `nprod` is deployed. Environments of private repositories require
a paid GitHub plan.

## 4. Deploy

Push to `master`, or open **Actions > Deploy demo > Run workflow**: the run deploys
`nprod` and runs its jobs, then does the same for `prod`. If a job of the workflow fails,
including a run of the jobs of the data products, the following ones do not run; fix the
cause and run the workflow again.

In each deployment job, the logs of the **Deploy with Pulumi** step list the created
resources and end with the URLs of the jobs, by data product (`job_urls`). The jobs create
the quality dashboards and the Genie spaces, in the `dashboards` and `genie` folders.

## 5. Run the pipeline

Each deployment runs the jobs of its environment (see
[How the workflow runs](#how-the-workflow-runs)). To run one again, open **Jobs &
Pipelines > cds-demo-req-nprod** in Databricks and select **Run now**. The first run
creates the tables of `nprod.donnees_qc_req`, and the first run of
**cds-demo-repertoire_municipalites-nprod** those of
`nprod.donnees_qc_repertoire_municipalites`. Each run then:

1. creates or updates the tables from their contracts, with the tags of their columns
   (schema);
2. loads them from the CSV files, with the names and types of their contracts
   (transformation): the synthetic files in `nprod`, the published ones in `prod`;
3. documents and tags them, and their history tables, and in `prod` places them in their
   Discover domain (metadata);
4. checks them with DQX: each row gets its `_errors` and `_warnings` (quality);
5. updates their history in the `<table>_historique` tables, created with the tags of
   their columns, and deletes the versions replaced more than 10 years ago (scd);
6. creates or updates the Genie space of the data product, in the `genie` folder, and in
   `prod` places it in the Discover domain of the tables (genie);
7. creates or updates the quality dashboard, publishes it, in the `dashboards` folder,
   and in `prod` places it in the Discover domain of the tables (dashboard).

The columns of a new table, or of a new history table, are tagged before any data is
written to it: its classified columns are masked from the first run. If they cannot be
tagged, the step drops the new table and fails.

The quality dashboard then shows the results of the quality step. To query them
directly, for example the rows with quality errors:

```sql
SELECT numero_entreprise, numero_etablissement, _errors
FROM nprod.donnees_qc_req.etablissement
WHERE size(_errors) > 0;
```

The addresses are masked for the users, and not for the service principal. As a user,
this query finds a single address, `****`, in each table:

```sql
SELECT count(DISTINCT adresse_ligne_1) AS distinct_addresses
FROM nprod.donnees_qc_req.etablissement_historique;
```

Run as the service principal, it finds the actual addresses. The quality dashboard runs
as the service principal, but shows no address: run the query with the
[Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/), authenticated as the
service principal, on the SQL warehouse of the dashboard (its id is on the **SQL
Warehouses** page):

```bash
export DATABRICKS_HOST=https://<workspace>
export DATABRICKS_CLIENT_ID=bf175dc4-c86b-4e88-83e9-9194c485a080
export DATABRICKS_CLIENT_SECRET=...
databricks api post /api/2.0/sql/statements --json '{"warehouse_id": "<id>", "wait_timeout": "30s",
  "statement": "SELECT count(DISTINCT adresse_ligne_1) AS distinct_addresses FROM nprod.donnees_qc_req.etablissement_historique"}'
```

## Variables

The stack files are the variables of the deployment.

| Key | Description |
| --- | --- |
| `databricks:host` | URL of the workspace. |
| `databricks:clientId` | Application id of the deployment service principal; the jobs run as it. |
| `cds-demo:environment` | CDS environment id: `nprod` or `prod`. It is also the catalog of the tables. |
| `cds-demo:sourceRoot` | Folder of the folders of the CSV files, one per data product: `<sourceRoot>/<group>/<data product>`. **Placeholder**: `/Volumes/ext`. |
| `cds-demo:syntheticData` | `true`: the deployment uploads the synthetic data set to the folders of its data products and their jobs load it, the other jobs loading their published files; `false`: the jobs load the published files. `true` in `nprod`, `false` in `prod`. |
| `cds-demo:createCatalog` | `true`: the deployment creates the catalog of the environment, which needs a storage location (see [Creating the catalog with Pulumi](#creating-the-catalog-with-pulumi)). `false` in both stacks: the account uses Default Storage. |
| `cds-demo:catalogStorageRoot` | Optional. Storage location of the created catalog, when the metastore has none. |
| `cds-demo:catalogReaders` | Optional. List of the users or groups granted read access to the created catalog. |
| `cds-demo:workspaceRoot` | Workspace folder of the deployed files. |
| `cds-demo:environmentVersion` | Serverless environment version of the job. |
| `cds-demo:dqxVersion` | Version of DQX installed in the job environment. |
| `cds-demo:maxWorkers` | Number of tables processed in parallel by each service. |
| `cds-demo:warehouseName` | SQL warehouse of the quality dashboard, the Genie space and the alerts. **Placeholder**: `Serverless Starter Warehouse`. |
| `cds-demo:masking` | `true`: the deployment masks the classified columns (see [Masking of the classified columns](#masking-of-the-classified-columns)). `true` in both stacks. |
| `cds-demo:maskedClassifications` | Optional. List of the classifications masked, among the values of the manifest: `["restricted"]` by default, `[]` for none. |
| `cds-demo:maskingExemptPrincipals` | Optional. List of the users or groups reading the masked columns as they are, besides the service principal. |
| `cds-demo:createTagPolicies` | `true`: the deployment creates the governed tag `classification`. It belongs to the account: `true` in `nprod` only. |
| `cds-demo:createDomains` | `true`: the deployment creates the Discover domains of the contracts and their governed tags (see [Discover domains](#discover-domains)). They belong to the account: `true` in `nprod` only. |
| `cds-demo:genieUsers` | Optional. List of the users, groups or service principals (application ids) granted `CAN RUN` on the `genie` folder, and so on the Genie space: `["users"]` in both stacks. None by default: the folder then only has the permissions it inherits (see [Genie space](#genie-space)). |
| `cds-demo:timezone` | Time zone of the schedules of the jobs and of the alerts: `America/Toronto`. |
| `cds-demo:pauseSchedule` | `true`, the default: the schedules of the jobs are paused, and the jobs run on demand. `true` in `nprod`, `false` in `prod`. |

## Deploy from your computer (optional)

1. Install the [Pulumi CLI](https://www.pulumi.com/docs/iac/download-install/) and run
   `pulumi login`.
2. From the root of the repository, build the wheel:

   ```bash
   uv build --package cds-framework --wheel --out-dir dist
   ```

3. Authenticate to Databricks, with the secret of the service principal
   (`export DATABRICKS_CLIENT_SECRET=...`), and optionally set the addresses receiving
   the alerts (`export ALERT_EMAILS=you@example.com`).
4. Deploy:

   ```bash
   cd cds-demo/deploy
   uv sync
   pulumi stack select nprod --create
   pulumi preview   # shows the changes
   pulumi up        # applies them
   ```

`pulumi destroy` removes the job, the alerts and their notification destination, the
files, the `dashboards` and `genie` folders with the dashboard and the Genie space, and
the masking (policy,
function, `governance` schema). It does not drop the tables. The governed tag
`classification` created by the `nprod` stack is protected, because the `prod` masking
uses it: `pulumi destroy` of `nprod` fails until you run `pulumi state unprotect` on it,
or `pulumi state delete` to keep the tag and forget it. The same goes for the Discover
domains and their governed tags, which the `prod` tables use. A catalog created by the
deployment (`createCatalog`) is protected too, so that its data is not deleted by mistake.

## Tests

The program is tested with the mocks of Pulumi, and [run_jobs.py](run_jobs.py) with a
fake of the jobs API, without the CLI nor a workspace:

```bash
cd cds-demo/deploy
uv run pytest
```
