-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT * ,concat(driver_Ref,'_',code ) as new_driver_ref
FROM drivers;

-- COMMAND ----------

SELECT * ,split(name,' ' )[0] forename,split(name,' ' )[1] surname
FROM drivers;

-- COMMAND ----------

SELECT *, current_timestamp()
FROM drivers;

-- COMMAND ----------

SELECT *, date_format(dob,'dd-MM-yyyy' )
FROM drivers;

-- COMMAND ----------

SELECT *, date_add(dob, 1)
FROM drivers;

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers;

-- COMMAND ----------

SELECT MAX(DOB)
FROM drivers;

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers
WHERE nationality='British';

-- COMMAND ----------

SELECT nationality,COUNT(*)
FROM drivers
GROUP BY nationality
ORDER BY nationality;


-- COMMAND ----------

SELECT nationality,COUNT(*)
FROM drivers
GROUP BY nationality
HAVING COUNT(*)>100
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality,name,dob, rank() OVER (PARTITION BY nationality ORDER BY dob desc) as age_rank
FROM drivers
ORDER BY nationality,age_rank;



-- COMMAND ----------


