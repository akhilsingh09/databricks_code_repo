# Databricks notebook source
# MAGIC %md
# MAGIC ### PRODUCE DRIVER STANDINGS

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read\
    .format("parquet")\
    .load(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date = '{v_file_date}'")\
    .select("race_year")\
    .distinct() \
    .collect()

# COMMAND ----------

race_results_list

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read\
    .format("parquet")\
    .load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, count, col, when


driver_standings_df = race_results_df\
    .groupBy("race_year", "driver_name", "driver_nationality" ,"team" )\
    .agg(sum("points").alias("total_points"), count(when(col("position")== 1, True    )).alias("wins")   )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# final_df.write\
#     .format("parquet")\
#     .mode("overwrite")\
#     .saveAsTable("f1_presentation.driver_standings")

overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.driver_standings