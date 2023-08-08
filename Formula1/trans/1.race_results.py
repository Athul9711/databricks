# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

drivers_df=spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df=spark.read.format("delta").load(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name","team")

# COMMAND ----------

circuits_df=spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df=spark.read.format("delta").load(f"{processed_folder_path}/races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df=spark.read.format("delta").load(f"{processed_folder_path}/results")\
.filter(f"file_date='{v_file_date}'")\
    .withColumnRenamed("time","race_time")\
    .withColumnRenamed("race_id","result_race_Id")\
    .withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df,races_df.circuit_Id==circuits_df.circuit_Id,"inner")\
    .select(races_df.race_Id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results_df=results_df.join(race_circuits_df, results_df.result_race_Id==race_circuits_df.race_Id)\
                          .join(drivers_df, results_df.driver_Id==drivers_df.driver_Id)\
                          .join(constructors_df ,results_df.constructor_Id==constructors_df.constructor_Id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=race_results_df.select("race_Id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date")\
.withColumn("create_date",current_timestamp())\
.withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

# %sql
# DROP TABLE f1_presentation.race_results

# COMMAND ----------

# display(final_df).filter("race_year==2020 and race_name=='Abu Dhabi Grand Prix'").orderby(final_df.points.desc())

# COMMAND ----------

# final_df.write.mode('overwrite').partitionBy('race_Id').format("delta").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_Id')

# COMMAND ----------

#  %sql
# DROP TABLE f1_presentation.race_results

# COMMAND ----------

# %sql
# SELECT race_Id, count(1)
# FROM f1_presentation.race_results
# GROUP BY race_Id
# ORDER BY race_Id DESC;

 

# COMMAND ----------

merge_condition='tgt.driver_name=src.driver_name AND tgt.race_id=src.race_id '
merge_delta_data(final_df,'f1_presentation','race_results',presentation_folder_path,merge_condition,'race_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_Id
# MAGIC ORDER BY race_Id DESC

# COMMAND ----------


