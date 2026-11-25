# Databricks notebook source
# MAGIC %md
# MAGIC # CDS - Publication
# MAGIC
# MAGIC Runs the genie service, the dashboard service, then the pages service, in one task: creates or updates the Genie space, the quality dashboard and the Pages (the business definitions of the Discover domains, used by Genie Ontology) of a data product from its data contracts. Run it after the metadata service, which sets the comments of the tables, and the quality service, which adds its result columns. A failed service stops the notebook: the next ones are not run.
# MAGIC
# MAGIC Combining them saves a task in a job: Databricks Free Edition refuses the runs of a job of more than 5 tasks.
# MAGIC
# MAGIC | Parameter | Required | Description |
# MAGIC |---|---|---|
# MAGIC | `environment` | Yes | Environment id, e.g. `nprod` or `prod`. |
# MAGIC | `roots` | Yes | Comma-separated root folders of the data products. |
# MAGIC | `data_product` | Yes | Data product to process: `<group>.<data_product>`. |
# MAGIC | `genie_warehouse_id` | Yes | Id of the SQL warehouse running the queries of the space. |
# MAGIC | `genie_parent_path` | No | Workspace folder of the space, e.g. `/Workspace/Shared/genie`; the home folder of the user by default. |
# MAGIC | `dashboard_warehouse_id` | Yes | Id of the SQL warehouse running the queries of the dashboard. |
# MAGIC | `dashboard_parent_path` | No | Workspace folder of a new dashboard, e.g. `/Workspace/Shared/dashboards`; the home folder of the user by default. |
# MAGIC
# MAGIC Other parameters are available to the configuration of the data products, through the
# MAGIC `RuntimeConfiguration`. The `cds-framework` package must be installed in the environment
# MAGIC of the notebook.

# COMMAND ----------

from cds import DashboardService, GenieService, PagesService, run_service

# The parameters of the genie and dashboard services, besides those of every service: set
# them to run the notebook by hand. The job parameters take precedence.
dbutils.widgets.text("genie_warehouse_id", "", "SQL warehouse id of the space")
dbutils.widgets.text("genie_parent_path", "", "Folder of the space")
dbutils.widgets.text("dashboard_warehouse_id", "", "SQL warehouse id of the dashboard")
dbutils.widgets.text("dashboard_parent_path", "", "Folder of the dashboard")

run_service(GenieService, spark=spark, dbutils=dbutils)
run_service(DashboardService, spark=spark, dbutils=dbutils)
run_service(PagesService, spark=spark, dbutils=dbutils)
