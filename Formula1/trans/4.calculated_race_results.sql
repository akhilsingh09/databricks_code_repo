-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT
  races.race_year,
  constructors.name AS tem_name,
  drivers.name AS driver_name,
  results.position,
  results.points,
  11 - results.position AS calculated_points
FROM f1_processed.results
JOIN f1_processed.drivers ON drivers.driver_id = results.driver_id
JOIN f1_processed.constructors ON results.constructor_id = constructors.constructor_id
JOIN f1_processed.races ON races.race_id = results.race_id
WHERE results.position <= 10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

