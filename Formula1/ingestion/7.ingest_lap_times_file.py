# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), True),
                               StructField("lap",IntegerType(),True),
                               StructField("position", IntegerType(), True),
                               StructField("time", StringType(), True),
                               StructField("milliseconds", IntegerType(), True)
                        
])

# COMMAND ----------

lap_times_df=spark.read\
.schema(lap_times_schema)\
.csv("/mnt/formula1dl987654/raw/lap_times")

# COMMAND ----------

# display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df=lap_times_df.withColumnRenamed("raceId","race_Id")\
                                  .withColumnRenamed("driverId","driver_Id")\
                                  .withColumn("ingestion_date",current_timestamp())\
                                  .withColumn("file_date",lit(v_file_date))\
                                  .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

merge_condition='tgt.race_id=src.race_id AND tgt.driver_Id=src.driver_Id AND tgt.lap=src.lap AND tgt.race_id=src.race_id '
merge_delta_data(final_df,'f1_processed','lap_times',processed_folder_path,merge_condition,'race_Id')

# COMMAND ----------

# display(spark.read.parquet("/mnt/formual1dl987654/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.lap_times
# MAGIC
