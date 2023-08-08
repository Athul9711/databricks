# Databricks notebook source
dbutils.widgets.text("p_file_date","2023-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
                CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
                (
                race_year INT,
                team_name STRING,
                driver_Id INT,
                driver_name STRING,
                race_Id INT,
                position INT,
                points INT,
                calculated_points INT,
                created_date TIMESTAMP,
                updated_date TIMESTAMP
                )      
                USING DELTA
          
          """)

# COMMAND ----------

# %sql
# SELECT * FROM f1_processed.results

# COMMAND ----------

spark.sql(f"""
                CREATE OR REPLACE TEMP VIEW race_result_updated
                AS
                SELECT 
                rc.race_year
                ,c.name team_name
                ,d.driver_Id
                ,d.name driver_name
                ,r.race_Id
                ,r.position
                ,r.points
                ,(11-r.position) as calculated_points
                FROM f1_processed.results r
                JOIN f1_processed.drivers d on (r.driver_Id=d.driver_Id)
                JOIN f1_processed.constructors c on (r.constructor_Id=c.constructor_Id)
                JOIN f1_processed.races rc on  (r.race_Id=rc.race_Id)
                WHERE r.position<=10 AND r.file_date='{v_file_date}'
        """)

# COMMAND ----------

# %sql
# SELECT * FROM race_result_updated

# COMMAND ----------

spark.sql(f"""
            MERGE INTO f1_presentation.calculated_race_results tgt
            USING race_result_updated upd
            ON (tgt.driver_Id=upd.driver_Id AND tgt.race_Id=upd.race_Id)
            WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           --tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
            WHEN NOT MATCHED
                THEN INSERT(race_year,team_name,driver_Id,driver_name,race_Id,position,calculated_points,created_date) --To add points
                     VALUES(race_year,team_name,driver_Id,driver_name,race_Id,position,calculated_points,current_timestamp) --To add points
        """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_result_updated

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results

# COMMAND ----------


