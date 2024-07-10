# Databricks notebook source
display(dbutils.fs.ls("abfss://demo@dlformula13213.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dlformula13213.dfs.core.windows.net"))

# COMMAND ----------

