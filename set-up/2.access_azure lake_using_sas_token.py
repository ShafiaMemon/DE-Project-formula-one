# Databricks notebook source
# MAGIC %md
# MAGIC #### Access azure data lake using sas token
# MAGIC 1. set spark config
# MAGIC 1. list file from demo container
# MAGIC 1. read data from csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalformulaone.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalformulaone.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalformulaone.dfs.core.windows.net", "sp=rl&st=2024-09-09T12:21:43Z&se=2024-09-09T20:21:43Z&spr=https&sv=2022-11-02&sr=c&sig=hAX%2B6u8eFvy7k8KUxXfahWt9xrhRL1C%2FGL61xYUUkZU%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@datalformulaone.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@datalformulaone.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@datalformulaone.dfs.core.windows.net"))

# COMMAND ----------

