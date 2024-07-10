-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Teams of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT
  tem_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points,
  rank() OVER (ORDER BY round(avg(calculated_points), 3) DESC) team_rank
FROM f1_presentation.calculated_race_results
GROUP BY tem_name
HAVING total_races >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

select * from v_dominant_teams

-- COMMAND ----------

SELECT
  race_year,
  tem_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points
FROM f1_presentation.calculated_race_results
WHERE tem_name IN (SELECT tem_name FROM v_dominant_teams  WHERE team_rank <= 5 )
GROUP BY tem_name, race_year
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT
  race_year,
  tem_name,
  count(1) as total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points), 3) as avg_points
FROM f1_presentation.calculated_race_results
WHERE tem_name IN (SELECT tem_name FROM v_dominant_teams  WHERE team_rank <= 5 )
GROUP BY tem_name, race_year
ORDER BY race_year, avg_points DESC