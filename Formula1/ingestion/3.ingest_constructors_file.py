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

from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

ddl_schema = """ constructorId Integer,
                constructorRef String,
                name String,
                nationality String,
                url String
                """

# COMMAND ----------

schema = StructType([
    StructField("constructorId",IntegerType()),    
    StructField("constructorRef", StringType()),    
    StructField("name", StringType()),
    StructField("nationality", StringType()),
    StructField("url", StringType())

])

# COMMAND ----------

constructors_df = spark.read\
    .format("json")\
    .schema(ddl_schema)\
    .load(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop url columna and add ingestion_date column to df (Alsoe format columns)

# COMMAND ----------

constructors_formated_df = constructors_df\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("constructorRef", "constructor_ref")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))\
    .drop("url")

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_formated_df)

# COMMAND ----------

constructors_final_df.write\
    .format("parquet")\
    .mode("overwrite")\
    .saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

