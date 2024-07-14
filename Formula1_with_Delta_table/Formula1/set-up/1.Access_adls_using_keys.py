# Databricks notebook source
formula1dl_account_key = dbutils.secrets.get(scope='formaula1-scope', key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dlformula13213.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformula13213.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dlformula13213.dfs.core.windows.net"))

# COMMAND ----------

