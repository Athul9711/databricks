-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM drivers
LIMIT 100;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT NAME, dob AS date_of_birth
FROM drivers WHERE nationality='British'
AND dob>='1990-01-01'
ORDER BY dob DESC;

-- COMMAND ----------

SELECT *
FROM drivers 
ORDER BY nationality ASC,DOB DESC; 

-- COMMAND ----------

SELECT NAME, dob AS date_of_birth,nationality
FROM drivers WHERE (nationality='British'
AND dob>='1990-01-01') OR nationality='Indian'
ORDER BY dob DESC;

-- COMMAND ----------


