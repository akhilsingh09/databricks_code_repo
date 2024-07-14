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

qualifying_schema = StructType([
    StructField("qualifyId", IntegerType()),
    StructField("raceId", IntegerType()),
    StructField("driverId", IntegerType()),
    StructField("constructorId", IntegerType()),
    StructField("number", IntegerType()),
    StructField("position", IntegerType()),
    StructField("q1", StringType()),
    StructField("q2", StringType()),
    StructField("q3", StringType())
])

# COMMAND ----------

qualifying_df = spark.read\
    .format("json")\
    .schema(qualifying_schema)\
    .option("multiLine", "true")\
    .load(f"{raw_folder_path}/{v_file_date}/qualifying/*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add new column 
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current_timestamp
# MAGIC

# COMMAND ----------

qualifying_renamed_df = qualifying_df\
    .withColumnRenamed("qualifyId", "qualify_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumn("source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# qualifying_final_df.write\
#     .format("parquet")\
#         .mode("overwrite")\
#             .saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# overwrite_partition(qualifying_final_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")