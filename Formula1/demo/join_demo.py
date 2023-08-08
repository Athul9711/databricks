# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019")\
    .withColumnRenamed("name","race_name")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
      .filter("circuit_Id<70")\
      .withColumnRenamed("name","circuits_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df, circuits_df.circuit_Id==races_df.circuit_Id, "inner")\
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df, circuits_df.circuit_Id==races_df.circuit_Id, "left")\
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df, circuits_df.circuit_Id==races_df.circuit_Id, "right")\
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df, circuits_df.circuit_Id==races_df.circuit_Id, "full")\
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df, circuits_df.circuit_Id==races_df.circuit_Id, "semi")\
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df, circuits_df.circuit_Id==races_df.circuit_Id, "anti")\
    

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df=races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

int(races_df.count()) * int(circuits_df.count()) 

# COMMAND ----------


