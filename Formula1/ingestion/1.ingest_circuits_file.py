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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId", IntegerType(),False),
                                   StructField("circuitRef", StringType(),False),
                                   StructField("name", StringType(),False),
                                   StructField("location", StringType(),False),
                                   StructField("country", StringType(),False),
                                   StructField("lat", DoubleType(),False),
                                   StructField("lng", DoubleType(),False),
                                   StructField("alt", IntegerType(),False),
                                   StructField("url", StringType(),False),
                                   

])

# COMMAND ----------

circuits_df=spark.read\
.option("header",True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"), col("name"), col("location"),col("country"), col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitid","circuit_Id")\
    .withColumnRenamed("circuitid","circuit_Id")\
     .withColumnRenamed("circuitid","circuit_Id")\
     .withColumnRenamed("circuitid","circuit_Id")\
     .withColumnRenamed("circuitid","circuit_Id")\
     .withColumnRenamed("circuitid","circuit_Id")\
     .withColumn("data_source", lit(v_data_source))\
     .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# display(circuits_final_df)

# COMMAND ----------

# %fs
# ls /mnt/formula1dl987654/processed/circuits

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")
