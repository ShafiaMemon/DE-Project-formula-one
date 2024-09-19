# Databricks notebook source
formula1dldemo_sas_token = dbutils.secrets.get(scope = 'formulaone-scope', key = 'formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalformulaone.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalformulaone.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalformulaone.dfs.core.windows.net",formula1dldemo_sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@datalformulaone.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@datalformulaone.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@datalformulaone.dfs.core.windows.net"))