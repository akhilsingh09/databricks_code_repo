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

pit_stope_schema = StructType([
    StructField("raceId", IntegerType()),
    StructField("driverId", IntegerType()),
    StructField("stop", IntegerType()),
    StructField("lap", IntegerType()),
    StructField("time", StringType()),
    StructField("duration", StringType()),
    StructField("milliseconds", IntegerType())
])

# COMMAND ----------

pit_stope_df = spark.read\
    .format("json")\
    .schema(pit_stope_schema)\
    .option("multiLine", "true")\
    .load(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add new column 
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current_timestamp
# MAGIC

# COMMAND ----------

pit_stope_renamed_df = pit_stope_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumn("source",lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

pit_stope_final_df = add_ingestion_date(pit_stope_renamed_df)

# COMMAND ----------

# pit_stope_final_df.write\
#     .format("parquet")\
#         .mode("overwrite")\
#             .saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# overwrite_partition(pit_stope_final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stope_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC * 
# MAGIC FROM f1_processed.pit_stops
# MAGIC