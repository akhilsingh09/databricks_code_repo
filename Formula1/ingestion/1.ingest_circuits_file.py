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

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit

# COMMAND ----------

# Schema defination using StructType and StructField
ddl_schema = StructType([
    StructField("circuitId", IntegerType()),
    StructField("circuitRef", StringType()),
    StructField("name", StringType()),
    StructField("location", StringType()),
    StructField("country", StringType()),
    StructField("lat", DoubleType()),
    StructField("lng", DoubleType()),
    StructField("alt", IntegerType()),
    StructField("url", StringType())
])

# COMMAND ----------

# Schema defination using DDL 
circuit_schema = """circuitId Integer, 
                    circuitRef String, 
                    name String, 
                    location String, 
                    country String, 
                    lat Double, 
                    lng Double, 
                    alt Integer, 
                    url String  """

# COMMAND ----------

# DBTITLE 1,Untitled Code
circuit_df = spark.read\
            .format("csv")\
            .option("header", "true")\
            .schema(ddl_schema)\
            .load(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuit_selected_df = circuit_df.select(col("circuitId"),col("circuitRef"),
                                        col("name"),col  ("location"),col("country"),
                                        col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id")\
                        .withColumnRenamed("circuitRef", "circuit_ref")\
                        .withColumnRenamed("lat", "latitude")\
                        .withColumnRenamed("lng", "longitude")\
                        .withColumnRenamed("alt", "altitude")\
                        .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

"""circuit_final_df = circuit_renamed_df\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("env", lit("Prod"))"""

circuit_final_df = add_ingestion_date(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as Parquet

# COMMAND ----------

circuit_final_df.write\
    .format("parquet")\
    .mode("overwrite")\
    .saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(spark.read.format("parquet").load(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

