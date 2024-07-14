# Databricks notebook source
display(dbutils.fs.ls('dbfs:/mnt/formula132213/'))

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read\
    .format("parquet")\
    .load(f"{processed_folder_path}/races")\
        .filter("race_year = 2019")\
            .withColumnRenamed("name", "race_name")
display(races_df)

# COMMAND ----------

circuits_df = spark.read\
    .format("parquet")\
    .load(f"{processed_folder_path}/circuits")\
    .filter("circuit_id < 70")\
    .withColumnRenamed("name", "circuit_name")
display(circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df\
    .join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner" )\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### OUTER JOINS

# COMMAND ----------

# Left Outer Join

race_circuits_df = circuits_df\
    .join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left" )\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(race_circuits_df)

# COMMAND ----------

# Right Outer Join
race_circuits_df = circuits_df\
    .join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right" )\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(race_circuits_df)

# COMMAND ----------

# Full Outer Join
race_circuits_df = circuits_df\
    .join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full" )\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SEMI JOINS

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI JOINS

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### CROSS JOINS

# COMMAND ----------

