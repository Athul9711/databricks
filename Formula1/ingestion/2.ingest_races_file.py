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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId", IntegerType(), False),
                               StructField("year", IntegerType(), False),
                               StructField("round", IntegerType(), False),
                               StructField("circuitId", IntegerType(), False),
                               StructField("name", StringType(), False),
                               StructField("date", DateType(), False),
                               StructField("time", StringType(), False),
                               StructField("url", StringType(), False)
                        
])

# COMMAND ----------

races_df=spark.read\
.option("header", True)\
.schema(races_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# display(races_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, col, lit, concat

# COMMAND ----------

races_with_timestamp_df=races_df.withColumn("ingestion_date", current_timestamp())\
                        .withColumn("race_timeStamp", to_timestamp(concat(col("date"),lit(' '), col("time")),"yyyy-MM-dd HH:mm:ss"))\
                        .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------


# display(races_with_timestamp_df)

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col("raceId").alias("race_Id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_Id"),col("name"),col("ingestion_date"),col("race_timeStamp"),col("data_source"),col("file_date"))

# COMMAND ----------

# display(races_selected_df)

# COMMAND ----------

# races_selected_df.write.mode('overwrite').format("parquet").saveAsTable('f1_processed.races')

# COMMAND ----------

# %fs
# ls /mnt/formula1dl987654/processed/races

# COMMAND ----------

# display(spark.read.parquet('/mnt/formula1dl987654/processed/races'))

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').format("delta").saveAsTable('f1_processed.races')

# COMMAND ----------

# display(spark.read.parquet('/mnt/formula1dl987654/processed/races'))

# COMMAND ----------

# %sql
# SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")
