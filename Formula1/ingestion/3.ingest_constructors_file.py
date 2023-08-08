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

constructors_schema="constructorId INT, constructorRef STRING, name STRING, url STRING "

# COMMAND ----------

constructors_df=spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

# display(constructors_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_dropped_df=constructors_df.drop(col('url'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_final_df=constructors_dropped_df.withColumnRenamed("constructorId", "constructor_Id")\
                                            .withColumnRenamed("constructorRef", "constructor_Ref")\
                                            .withColumn("ingeston_date",current_timestamp())\
                                            .withColumn("data_source", lit(v_data_source))\
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# display(constructors_final_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")


# COMMAND ----------

# %fs
# ls /mnt/formula1dl987654/processed/constructors

# COMMAND ----------

# display(spark.read.parquet('/mnt/formula1dl987654/processed/constructors'))

# COMMAND ----------

# %sql
# SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")
