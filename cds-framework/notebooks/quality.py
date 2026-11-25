# Databricks notebook source
# MAGIC %md
# MAGIC # CDS - Quality service (DQX)
# MAGIC
# MAGIC Checks the tables of a data product with DQX. The checks are inferred from the data contracts (required columns, keys, relationships, allowed values...) and completed by the configured ones. The results are written in the `_errors` and `_warnings` columns of each row, and the statistics of the tables and of their checks, against the thresholds of the contracts, are appended to the `_quality_summary` table of their schema.
# MAGIC
# MAGIC | Parameter | Required | Description |
# MAGIC |---|---|---|
# MAGIC | `environment` | Yes | Environment id, e.g. `nprod` or `prod`. |
# MAGIC | `roots` | Yes | Comma-separated root folders of the data products. |
# MAGIC | `data_product` | Yes | Data product to process: `<group>.<data_product>`. |
# MAGIC | `tables` | No | Comma-separated tables to process; all the tables by default. |
# MAGIC | `max_workers` | No | Number of tables processed in parallel; 4 by default. |
# MAGIC | `run_id` | No | Identifies the run in the quality summary, e.g. `{{job.run_id}}`. |
# MAGIC
# MAGIC Other parameters are available to the configuration of the data products, through the
# MAGIC `RuntimeConfiguration`. The `cds-framework` package must be installed in the environment
# MAGIC of the notebook.

# COMMAND ----------

from cds import QualityService, run_service

run_service(QualityService, spark=spark, dbutils=dbutils)
