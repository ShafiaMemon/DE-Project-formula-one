# Databricks notebook source
# MAGIC %md
# MAGIC #### Access azure data lake using access keys
# MAGIC 1. set spark config
# MAGIC 1. list file from demo container
# MAGIC 1. read data from csv file

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.datalformulaone.dfs.core.windows.net",
  "sec key"
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@datalformulaone.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@datalformulaone.dfs.core.windows.net"))

# COMMAND ----------

