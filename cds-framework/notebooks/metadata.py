# Databricks notebook source
# MAGIC %md
# MAGIC # CDS - Metadata service
# MAGIC
# MAGIC Applies the descriptions of the data contracts as comments on the tables and columns of a data product.
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

from cds import run_service

run_service("metadata", spark=spark, dbutils=dbutils)
