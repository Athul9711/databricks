# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data lake using sas tokens
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1_demo_sas_token=dbutils.secrets.get(scope = 'formula1-scope',key='formla1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl987654.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl987654.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl987654.dfs.core.windows.net",formula1_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl987654.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dl987654.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl987654.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


