-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### DROP ALL TABLES

-- COMMAND ----------

DROP DATABASE if EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula132213/processed"

-- COMMAND ----------

DROP DATABASE if EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula132213/presentation"