-- Databricks notebook source
SELECT
  tem_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY tem_name
HAVING total_races >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  tem_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 and 2020
GROUP BY tem_name
HAVING total_races >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  tem_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 and 2011
GROUP BY tem_name
HAVING total_races >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

