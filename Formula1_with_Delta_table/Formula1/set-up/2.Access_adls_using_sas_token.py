# Databricks notebook source
resource_URL = "https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dlformula13213.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dlformula13213.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dlformula13213.dfs.core.windows.net", "sp=rl&st=2024-07-03T16:53:00Z&se=2024-07-04T00:53:00Z&spr=https&sv=2022-11-02&sr=c&sig=fkd0TIC9WVsP%2BKzjzWvBL5B2NrkxvGOI5%2FFNfhzVY7Q%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformula13213.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dlformula13213.dfs.core.windows.net"))

# COMMAND ----------

