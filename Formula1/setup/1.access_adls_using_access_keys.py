# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key=dbutils.secrets.get(scope='formula1-scope',key='formla1-demo-sas-token')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl987654.dfs.core.windows.net",
    formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl987654.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dl987654.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl987654.dfs.core.windows.net/circuits.csv"))
