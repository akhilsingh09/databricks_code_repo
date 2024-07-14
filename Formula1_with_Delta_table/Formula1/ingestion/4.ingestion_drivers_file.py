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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

name_schema = StructType([
    StructField("forename", StringType()),
    StructField("surname", StringType())
])

# COMMAND ----------

drivers_schema = StructType([
    StructField("driverId", IntegerType()),
    StructField("driverRef", StringType()),
    StructField("number", IntegerType()),
    StructField("code", StringType()),
    StructField("name", name_schema),
    StructField("dob", DateType()),
    StructField("nationality", StringType()),
    StructField("url", StringType())
])

# COMMAND ----------

drivers_df = spark.read\
    .format("json")\
        .schema(drivers_schema)\
            .load(f"{raw_folder_path}/{v_file_date}/drivers.json")


# COMMAND ----------

drivers_renamed_df = drivers_df\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("driverRef", "driver_ref")\
    .withColumn("name", concat(col("name.forename"),lit(" "),col("name.surname")))\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# drop url column

drivers_final_df = drivers_renamed_df.drop("url")

# COMMAND ----------

drivers_final2_df = add_ingestion_date(drivers_final_df)

# COMMAND ----------

drivers_final2_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")