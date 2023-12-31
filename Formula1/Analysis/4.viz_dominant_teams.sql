-- Databricks notebook source
-- MAGIC %python
-- MAGIC html="""<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams 
AS
SELECT 
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS Avg_points,
rank() over(order by AVG(calculated_points) DESC) AS team_rank
FROM
f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races>100
ORDER BY Avg_points DESC

-- COMMAND ----------

SELECT 
race_year,
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS Avg_points
FROM
f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams where team_rank<=10 )
GROUP BY race_year,team_name
ORDER BY race_year,Avg_points DESC

-- COMMAND ----------

SELECT 
race_year,
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS Avg_points
FROM
f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams where team_rank<=10 )
GROUP BY race_year,team_name
ORDER BY race_year,Avg_points DESC

-- COMMAND ----------


