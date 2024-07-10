# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read\
    .format("csv")\
    .schema(lap_times_schema)\
    .load(f"{raw_folder_path}/{v_file_date}/lap_times/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add new column 
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current_timestamp
# MAGIC

# COMMAND ----------

lap_times_renamed_df = lap_times_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumn("source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# lap_times_final_df.write\
#     .format("parquet")\
#         .mode("overwrite")\
#             .saveAsTable("f1_processed.lap_times")

# COMMAND ----------

overwrite_partition(lap_times_final_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")