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

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), True),
                               StructField("stop", StringType(), True),
                               StructField("lap",IntegerType(),True),
                               StructField("time", StringType(), True),
                               StructField("duration", StringType(), True),
                               StructField("milliseconds", IntegerType(), True)
                        
])

# COMMAND ----------

pit_stops_df=spark.read\
.schema(pit_stops_schema)\
.option("multiLine", True)\
.json("/mnt/formula1dl987654/raw/pit_stops.json")

# COMMAND ----------

# display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit

# COMMAND ----------

final_df=pit_stops_df.withColumnRenamed("raceId","race_Id")\
                                  .withColumnRenamed("driverId","driver_Id")\
                                  .withColumn("ingestion_date",current_timestamp())\
                                  .withColumn("file_date",lit(v_file_date))\
                                  .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")a

# COMMAND ----------

merge_condition='tgt.race_id=src.race_id AND tgt.driver_Id=src.driver_Id AND tgt.stop=src.stop AND tgt.race_id=src.race_id '
merge_delta_data(final_df,'f1_processed','pit_stops',processed_folder_path,merge_condition,'race_Id')

# COMMAND ----------

# display(spark.read.parquet("/mnt/formual1dl987654/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.pit_stops
# MAGIC
