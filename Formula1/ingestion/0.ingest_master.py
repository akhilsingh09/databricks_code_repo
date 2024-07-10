# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergest API", "p_file_date": "2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingestion_races_file", 0, {"p_data_source": "Ergest API", "p_file_date": "2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source": "Ergest API", "p_file_date": "2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingestion_drivers_file", 0, {"p_data_source": "Ergest API", "p_file_date": "2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingestion_results_file", 0, {"p_data_source": "Ergest API", "p_file_date": "2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingestion_pitstops_file", 0, {"p_data_source": "Ergest API", "p_file_date": "2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingestion_lap_times_folder", 0, {"p_data_source": "Ergest API", "p_file_date": "2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingestion_qualifying_folder", 0, {"p_data_source": "Ergest API", "p_file_date": "2021-04-18"})

# COMMAND ----------

print(v_result)