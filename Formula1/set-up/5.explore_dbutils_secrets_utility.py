# Databricks notebook source
dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------

dbutils.secrets.list(scope= 'formaula1-scope')


# COMMAND ----------

dbutils.secrets.get(scope='formaula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

