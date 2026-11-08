# Databricks notebook source
# MAGIC %md
# MAGIC # CDS - Schema service
# MAGIC
# MAGIC Creates the Unity Catalog schema of a data product and the tables described by its data contracts. Columns added to a contract are added to their table; a table whose columns were removed or retyped is replaced (see `SchemaSettings`).
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

from cds import SchemaService, run_service

run_service(SchemaService, spark=spark, dbutils=dbutils)
