# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2023-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                               StructField("raceId", IntegerType(), True),
                               StructField("driverId", IntegerType(), True),
                               StructField("constructorId", IntegerType(), True),
                               StructField("number",IntegerType(),True),
                               StructField("grid", IntegerType(), True),
                               StructField("position", IntegerType(), True),
                               StructField("position_text", StringType(), True),
                               StructField("position_order", IntegerType(), True),
                               StructField("points", FloatType(), True),
                               StructField("lap", IntegerType(), True),
                               StructField("time", StringType(), True),
                               StructField("milliseconds", IntegerType(), True),
                               StructField("fastestLap", IntegerType(), True),
                               StructField("rank", IntegerType(), True),
                               StructField("fastestLaptime", StringType(), True),
                               StructField("fastestLapSpeed", FloatType(), True),
                               StructField("statusId", StringType(), True),
                        
])

# COMMAND ----------

results_df=spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

#display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_with_columns_df=results_df.withColumnRenamed("resultId","result_Id")\
                                  .withColumnRenamed("raceId","race_Id")\
                                  .withColumnRenamed("driverId","driver_Id")\
                                  .withColumnRenamed("constructorId","constructor_Id")\
                                  .withColumnRenamed("positiontext","position_text")\
                                  .withColumnRenamed("positionorder","position_order")\
                                  .withColumnRenamed("fastestLap","fastest_Lap")\
                                  .withColumnRenamed("fastestLaptime","fastest_Lap_time")\
                                  .withColumnRenamed("fastestLapSpeed","fastest_Lap_Speed")\
                                  .withColumn("ingestion_date",current_timestamp())\
                                  .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

#display(results_with_columns_df)

# COMMAND ----------

results_final_df=results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

results_deduped_df=results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# for race_Id_list in results_final_df.select("race_Id").distinct().collect():
#      if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#          spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_Id={race_Id_list.race_Id})")

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_Id')

# COMMAND ----------

merge_condition='tgt.result_id=src.result_id AND tgt.race_id=src.race_id'
merge_delta_data(results_deduped_df,'f1_processed','results',processed_folder_path,merge_condition,'race_Id')

# COMMAND ----------

# %sql
# SELECT * FROM f1_processed.results

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_Id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.results

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dl987654/processed/results"))

# COMMAND ----------


dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_Id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC WHERE file_date='2021-03-21'
# MAGIC GROUP BY race_Id
# MAGIC HAVING COUNT(1)>1
# MAGIC  

# COMMAND ----------


