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

dbutils.widgets.text("p_file_date", "2021-03-21")

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType, StructType, StructField
from pyspark.sql.functions import col, to_timestamp, concat, lit, current_timestamp

# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)

])

# COMMAND ----------

races_df = spark.read\
        .format("csv")\
        .option("header", "true")\
        .schema(races_schema)\
        .load(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding race_timestamp to the dataframe ( by combining date and time columns)
# MAGIC #### and droppong column not required ("date", "time" and "url")

# COMMAND ----------

races_selected_df = races_df\
    .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(' '),col("time")),'yyyy-MM-dd HH:mm:ss')).drop("date","time","url")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))



# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("year", "race_year")\
    .withColumnRenamed("circuitId", "circuit_id")
    

# COMMAND ----------

races_final_df = add_ingestion_date(races_renamed_df)

# COMMAND ----------

races_final_df.write\
    .format("delta")\
    .mode("overwrite")\
    .partitionBy("race_year")\
    .saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")