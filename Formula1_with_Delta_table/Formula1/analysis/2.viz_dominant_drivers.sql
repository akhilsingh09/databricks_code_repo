-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Drivers of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT
  driver_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points,
  rank() OVER (ORDER BY round(avg(calculated_points), 3) DESC) driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  race_year,
  driver_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers  WHERE driver_rank <= 10 )
GROUP BY driver_name, race_year
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT
  race_year,
  driver_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers  WHERE driver_rank <= 10 )
GROUP BY driver_name, race_year
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT
  race_year,
  driver_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers  WHERE driver_rank <= 10 )
GROUP BY driver_name, race_year
ORDER BY race_year, avg_points DESC