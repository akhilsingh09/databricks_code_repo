# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read\
    .format("parquet")\
    .load(f"{processed_folder_path}/races")
display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5").show()

# COMMAND ----------

races_filtered_df = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))
display(races_filtered_df)

# COMMAND ----------
