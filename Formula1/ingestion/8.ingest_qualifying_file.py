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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                               StructField("raceId", IntegerType(), True),
                               StructField("driverId", IntegerType(), True),
                               StructField("constructorId", IntegerType(), True),
                               StructField("number",IntegerType(),True),
                               StructField("position", IntegerType(), True),
                               StructField("q1", StringType(), True),
                               StructField("q2", StringType(), True),
                               StructField("q3", StringType(), True)
                        
])

# COMMAND ----------

qualifying_df=spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json("/mnt/formula1dl987654/raw/qualifying")

# COMMAND ----------

# display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_Id")\
                                  .withColumnRenamed("raceId","race_Id")\
                                  .withColumnRenamed("driverId","driver_Id")\
                                  .withColumnRenamed("constructorId","constructor_Id")\
                                  .withColumn("ingestion_date",current_timestamp())\
                                  .withColumn("file_date",lit(v_file_date))\
                                  .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

merge_condition='tgt.qualify_Id=src.qualify_Id AND tgt.race_id=src.race_id '
merge_delta_data(final_df,'f1_processed','qualifying',processed_folder_path,merge_condition,'race_Id')

# COMMAND ----------

# display(spark.read.parquet("/mnt/formual1dl987654/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
