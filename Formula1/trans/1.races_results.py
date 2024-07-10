# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

drivers_df = processed_read("drivers")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = processed_read("constructors")\
    .withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = processed_read("circuits")\
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = processed_read("races")\
    .withColumnRenamed("name", "race_name")\
        .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = processed_read("results")\
    .filter(f"file_date = '{v_file_date}'")\
    .withColumnRenamed("time", "race_time")\
    .withColumnRenamed("race_id", "results_race_id")\
    .withColumnRenamed("file_date", "results_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOIN CIRCUITS TO RACES

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year,  races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOIN ALL OTHER DATAFRAMES

# COMMAND ----------

races_results_df = results_df.join(race_circuits_df, race_circuits_df.race_id == results_df.results_race_id)\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = races_results_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "results_file_date")\
       .withColumn("created_date", current_timestamp())\
       .withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

# display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix' ")\
#     .orderBy(final_df.points.desc()))

# COMMAND ----------

# final_df.write\
#     .format("parquet")\
#     .mode("overwrite")\
#     .saveAsTable("f1_presentation.race_results")


overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_presentation.race_results
# MAGIC WHERE race_year = 2021