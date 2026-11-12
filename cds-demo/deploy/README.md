# Deployment of the demo

The demo is deployed to Databricks by [Pulumi](https://www.pulumi.com/), run by a
GitHub Actions workflow. This guide sets everything up, step by step.

## What is deployed

The Pulumi program ([pipeline.py](pipeline.py)) creates, in the workspace folder
`/Shared/cds-demo/<environment>`:

```text
/Shared/cds-demo/nprod/
├── dashboards/      the quality dashboard (cds-demo/dashboards), published
├── notebooks/       the service notebooks of the framework (cds-framework/notebooks)
├── products/        the data products of the demo (cds-demo/products)
└── wheels/<hash>/   the wheel of the framework, built by the workflow
```

The quality dashboard, **REQ data quality (&lt;environment&gt;)**, shows the results of
the quality service. Its queries run on a SQL warehouse with the credentials of the
service principal, which owns the tables; it reads the tables of its environment.

It also creates the job `cds-demo-req-<environment>`, which runs the services of the
framework on serverless compute. The job has no schedule: you run it on demand.

```text
schema --> metadata
       --> transformation --> quality --> scd
```

The job runs as the deployment service principal. The tables are created in the
`<environment>` catalog, in the `donnees_qc_req` schema.

The two environments differ by their data and their catalog:

| | `nprod` | `prod` |
| --- | --- | --- |
| Data loaded by the job | the synthetic data set ([cds-demo/csv/testdata](../csv/testdata)), uploaded by the deployment next to the files of the register | the files of the register |
| Catalog | created by an administrator | created by an administrator with Default Storage, owned by the service principal |

The synthetic data set illustrates the quality service: 85 % of valid rows, 10 % with
warnings and 5 % with errors (see [cds-demo/README.md](../README.md#synthetic-data-set)).
The job receives the choice as its `synthetic_data` parameter.

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
- **Deploy nprod** runs `pulumi up` on the `nprod` stack, with that wheel.
- **Deploy prod** does the same on the `prod` stack, once `nprod` is deployed.

Until the secrets below are configured, the build runs and the deployments are skipped
with a warning instead of failing.

## 1. Prepare Databricks (once)

### Upload the data

Create a volume in the `ext` catalog and upload the six CSV files of the REQ data set
(`Entreprise.csv`, `Etablissements.csv`, `Nom.csv`, `FusionScissions.csv`,
`ContinuationsTransformations.csv` and `DomaineValeur.csv`), for example:

```sql
CREATE SCHEMA IF NOT EXISTS ext.donnees_qc;
CREATE VOLUME IF NOT EXISTS ext.donnees_qc.req;
```

Then, in **Catalog > ext > donnees_qc > req**, select **Upload to this volume**. If you use
another volume or folder, set `cds-demo:sourcePath` in both stack files.

The `nprod` deployment uploads the synthetic files (`*_testdata.csv`) to the same volume:
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
-- The files of the register, and the synthetic files uploaded by the nprod deployment.
GRANT USE CATALOG ON CATALOG ext TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
GRANT USE SCHEMA, READ VOLUME ON SCHEMA ext.donnees_qc TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
GRANT WRITE VOLUME ON VOLUME ext.donnees_qc.req TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
```

The service principal owns the `prod` catalog, and the schemas and tables it creates.
As a metastore administrator or the owner of the `nprod` catalog, you can query them. To
let other users read the tables, grant them `USE CATALOG` on the catalog, and `USE
SCHEMA` and `SELECT` on the schema `donnees_qc_req`.

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

The workflow passes `DATABRICKS_CLIENT_SECRET` to Pulumi as an environment variable.
The client id of the service principal is not secret: it is in the stack files.

GitHub masks secrets in the logs. If you later want an approval before deploying `prod`,
create a `prod` environment in **Settings > Environments** with required reviewers,
and add `environment: prod` to the `deploy-prod` job of the workflow: the run then waits
for the approval once `nprod` is deployed. Environments of private repositories require
a paid GitHub plan.

## 4. Deploy

Push to `master`, or open **Actions > Deploy demo > Run workflow**: the run deploys
`nprod`, then `prod`. If a job fails, the following ones do not run; fix the cause and
run the workflow again.

In each deployment job, the logs of the **Deploy with Pulumi** step list the created
resources and end with the URLs of the job (`job_url`) and of the quality dashboard
(`dashboard_url`).

## 5. Run the pipeline

In Databricks, open **Jobs & Pipelines > cds-demo-req-nprod** and select **Run now**. The
first run creates the tables of `nprod.donnees_qc_req`. Each run then:

1. creates or updates the tables from their contracts (schema);
2. documents them (metadata);
3. loads them from the CSV files, with the names and types of their contracts
   (transformation): the synthetic files in `nprod`, those of the register in `prod`;
4. checks them with DQX: each row gets its `_errors` and `_warnings` (quality);
5. updates their history in the `<table>_historique` tables (scd).

The quality dashboard then shows the results of the quality step. To query them
directly, for example the rows with quality errors:

```sql
SELECT numero_entreprise, numero_etablissement, _errors
FROM nprod.donnees_qc_req.etablissement
WHERE size(_errors) > 0;
```

## Variables

The stack files are the variables of the deployment.

| Key | Description |
| --- | --- |
| `databricks:host` | URL of the workspace. |
| `databricks:clientId` | Application id of the deployment service principal; the job runs as it. |
| `cds-demo:environment` | CDS environment id: `nprod` or `prod`. It is also the catalog of the tables. |
| `cds-demo:sourcePath` | Folder of the CSV files. **Placeholder**: `/Volumes/ext/donnees_qc/req`. |
| `cds-demo:syntheticData` | `true`: the deployment uploads the synthetic data set to `sourcePath` and the job loads it; `false`: the job loads the register. `true` in `nprod`, `false` in `prod`. |
| `cds-demo:createCatalog` | `true`: the deployment creates the catalog of the environment, which needs a storage location (see [Creating the catalog with Pulumi](#creating-the-catalog-with-pulumi)). `false` in both stacks: the account uses Default Storage. |
| `cds-demo:catalogStorageRoot` | Optional. Storage location of the created catalog, when the metastore has none. |
| `cds-demo:catalogReaders` | Optional. Users or groups granted read access to the created catalog. |
| `cds-demo:workspaceRoot` | Workspace folder of the deployed files. |
| `cds-demo:environmentVersion` | Serverless environment version of the job. |
| `cds-demo:dqxVersion` | Version of DQX installed in the job environment. |
| `cds-demo:maxWorkers` | Number of tables processed in parallel by each service. |
| `cds-demo:warehouseName` | SQL warehouse of the quality dashboard. **Placeholder**: `Serverless Starter Warehouse`. |
| `cds-demo:schema` | Optional. Schema of the tables, read by the dashboard: `donnees_qc_req` by default. |

## Deploy from your computer (optional)

1. Install the [Pulumi CLI](https://www.pulumi.com/docs/iac/download-install/) and run
   `pulumi login`.
2. From the root of the repository, build the wheel:

   ```bash
   uv build --package cds-framework --wheel --out-dir dist
   ```

3. Authenticate to Databricks, with the secret of the service principal
   (`export DATABRICKS_CLIENT_SECRET=...`).
4. Deploy:

   ```bash
   cd cds-demo/deploy
   uv sync
   pulumi stack select nprod --create
   pulumi preview   # shows the changes
   pulumi up        # applies them
   ```

`pulumi destroy` removes the job, the dashboard and the files. It does not drop the tables.
A catalog created by the deployment (`createCatalog`) is protected: `pulumi destroy`
fails until you run `pulumi state unprotect`, so that its data is not deleted by mistake.

## Tests

The program is tested with the mocks of Pulumi, without the CLI nor a workspace:

```bash
cd cds-demo/deploy
uv run pytest
```
