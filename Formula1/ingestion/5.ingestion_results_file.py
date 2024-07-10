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

v_file_date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

results_schema = StructType([
    StructField("resultId", IntegerType()),
    StructField("raceId", IntegerType()),
    StructField("driverId", IntegerType()),
    StructField("constructorId", IntegerType()),
    StructField("number", IntegerType()),
    StructField("grid", IntegerType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("positionOrder", IntegerType()),
    StructField("points", DoubleType()),
    StructField("laps", IntegerType()),
    StructField("time", StringType()),
    StructField("milliseconds", IntegerType()),
    StructField("fastestLap", IntegerType()),
    StructField("rank", IntegerType()),
    StructField("fastestLapTime", StringType()),
    StructField("fastestLapSpeed", StringType()),
    StructField("statusId", IntegerType())

])

# COMMAND ----------

results_df = spark.read\
    .format("json")\
        .schema(results_schema)\
            .load(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fix column names to standard naming convention

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId" , "driver_id")\
    .withColumnRenamed("constructorId" , "constructor_id")\
    .withColumnRenamed("driverId" , "driver_id")\
    .withColumnRenamed("positionText", "position_text")\
    .withColumnRenamed("positionOrder", "position_order")\
    .withColumnRenamed("fastestLap", "fastest_lap")\
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))    

# COMMAND ----------

results_final_df  = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop statusId column
# MAGIC

# COMMAND ----------

results_filtered_df = results_final_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data as parquet with partionby(race_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### METHOD 1

# COMMAND ----------

# for race_id_list in results_filtered_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_filtered_df.write\
#     .format("parquet")\
#     .mode("append")\
#     .partitionBy("race_id")\
#     .saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### METHOD 2

# COMMAND ----------

overwrite_partition(results_filtered_df, "f1_processed", "results", "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results;

# COMMAND ----------

# partition_column= "race_id"
# column_list = []
# for column_name in results_filtered_df.schema.names:
#     if column_name != partition_column:
#         column_list.append(column_name)
# column_list.append(partition_column)

# COMMAND ----------

# column_list

# COMMAND ----------

#Manually setting last column as "race_id" so that in delta files Partioning column will be considered as "race_id"
# results_filtered_df = results_filtered_df.select(column_list)

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_filtered_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_filtered_df.write.format("parquet").mode("overwrite").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

# This will be used for first file (cut over file)
# results_filtered_df.write\
#     .format("parquet")\
#     .mode("overwrite")\
#     .partitionBy("race_id")\
#     .saveAsTable("f1_processed.results")

# COMMAND ----------

# this is used to for next upcoming files (delta files)
# this will consider last column inserted in list as Partitioned Column
# results.filtered_Df.write\
#     .mode("overwrite")\
#     .insertInto("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC race_id,
# MAGIC count(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY 1
# MAGIC ORDER BY race_id DESC