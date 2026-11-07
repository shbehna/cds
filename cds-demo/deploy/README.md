# Deployment of the demo

The demo is deployed to Databricks by [Pulumi](https://www.pulumi.com/), run by a
GitHub Actions workflow. This guide sets everything up, step by step.

## What is deployed

The Pulumi program ([pipeline.py](pipeline.py)) creates, in the workspace folder
`/Shared/cds-demo/<environment>`:

```text
/Shared/cds-demo/nprod/
├── notebooks/       the service notebooks of the framework (cds-framework/notebooks)
├── products/        the data products of the demo (cds-demo/products)
└── wheels/<hash>/   the wheel of the framework, built by the workflow
```

It also creates the job `cds-demo-req-<environment>`, which runs the services of the
framework on serverless compute. The job has no schedule: you run it on demand.

```text
schema --> metadata
       --> transformation --> quality --> scd
```

The job runs as the deployment service principal. The tables are created in the
`<environment>` catalog, in the `donnees_qc_req` schema.

Pulumi keeps track of what it deployed in a *state*, stored by Pulumi Cloud, and
updates the workspace to match the program on each deployment. Each environment is a
Pulumi *stack*, `nprod` or `prod`, configured by [Pulumi.nprod.yaml](Pulumi.nprod.yaml) and
[Pulumi.prod.yaml](Pulumi.prod.yaml).

## How the workflow runs

The workflow [.github/workflows/deploy-demo.yml](../../.github/workflows/deploy-demo.yml):

- deploys `nprod` on each push to `master` that changes the framework or the demo;
- deploys `prod` when you run it manually, choosing `prod`.

It runs the tests, builds the wheel of the framework, then runs `pulumi up`. Until the
secrets below are configured, it skips the deployment with a warning instead of failing.

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

### Create a secret for the service principal

1. In the workspace, open **Settings > Identity and access > Service principals**.
2. Open the service principal `bf175dc4-c86b-4e88-83e9-9194c485a080`.
3. In **Secrets**, select **Generate secret** and copy the secret. It is shown only once.

### Grant access to the service principal

Run, as an administrator or owner of the catalogs:

```sql
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG nprod TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG prod TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
GRANT USE CATALOG ON CATALOG ext TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
GRANT USE SCHEMA, READ VOLUME ON SCHEMA ext.donnees_qc TO `bf175dc4-c86b-4e88-83e9-9194c485a080`;
```

The service principal owns the schemas and tables it creates. As the owner of the
catalogs, you can query them. To let other users read them, grant `USE SCHEMA` and
`SELECT` on the schema `donnees_qc_req`.

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

GitHub masks secrets in the logs. If you later want approvals before deploying `prod`,
create a `prod` environment in **Settings > Environments** with required reviewers,
and add `environment: ${{ inputs.environment || 'nprod' }}` to the job of the workflow.
Environments of private repositories require a paid GitHub plan.

## 4. Deploy

- **nprod**: push to `master`, or open **Actions > Deploy demo > Run workflow** and keep
  `nprod`.
- **prod**: open **Actions > Deploy demo > Run workflow** and choose `prod`.

The logs of the **Deploy with Pulumi** step list the created resources and end with the
URL of the job (`job_url`).

## 5. Run the pipeline

In Databricks, open **Jobs & Pipelines > cds-demo-req-nprod** and select **Run now**. The
first run creates the tables of `nprod.donnees_qc_req`. Each run then:

1. creates or updates the tables from their contracts (schema);
2. documents them (metadata);
3. loads them from the CSV files, with the names and types of their contracts (transformation);
4. checks them with DQX: each row gets its `_errors` and `_warnings` (quality);
5. updates their history in the `<table>_historique` tables (scd).

For example, to see the rows with quality errors:

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
| `cds-demo:workspaceRoot` | Workspace folder of the deployed files. |
| `cds-demo:environmentVersion` | Serverless environment version of the job. |
| `cds-demo:dqxVersion` | Version of DQX installed in the job environment. |
| `cds-demo:maxWorkers` | Number of tables processed in parallel by each service. |

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

`pulumi destroy` removes the job and the files. It does not drop the tables.

## Tests

The program is tested with the mocks of Pulumi, without the CLI nor a workspace:

```bash
cd cds-demo/deploy
uv run pytest
```
