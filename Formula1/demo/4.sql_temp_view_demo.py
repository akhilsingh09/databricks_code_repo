# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read\
    .format("parquet")\
    .load(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_results_2019_df =   spark.sql("SELECT * FROM v_race_results where race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

