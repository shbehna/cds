# Databricks notebook source
# MAGIC %md
# MAGIC # CDS - Load
# MAGIC
# MAGIC Runs the schema service, then the transformation service, in one task: creates the schema and the tables of a data product from its data contracts, then loads them. A table in error in the schema service stops the notebook: the transformation does not run.
# MAGIC
# MAGIC Combining them saves a task in a job: Databricks Free Edition refuses the runs of a job of more than 5 tasks.
# MAGIC
# MAGIC | Parameter | Required | Description |
# MAGIC |---|---|---|
# MAGIC | `environment` | Yes | Environment id, e.g. `nprod` or `prod`. |
# MAGIC | `roots` | Yes | Comma-separated root folders of the data products. |
# MAGIC | `data_product` | Yes | Data product to process: `<group>.<data_product>`. |
# MAGIC | `tables` | No | Comma-separated tables to process; all the tables by default. |
# MAGIC | `max_workers` | No | Number of tables processed in parallel; 4 by default. |
# MAGIC
# MAGIC Other parameters are available to the configuration of the data products, through the
# MAGIC `RuntimeConfiguration`. The `cds-framework` package must be installed in the environment
# MAGIC of the notebook.

# COMMAND ----------

from cds import SchemaService, TransformationService, run_service

run_service(SchemaService, spark=spark, dbutils=dbutils)
run_service(TransformationService, spark=spark, dbutils=dbutils)
