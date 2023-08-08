# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

client_id="07d77e3f-03cb-4502-8b91-4825c0c105eb"
tenant_id="e77d1219-ef73-4b66-9961-c613342ca237"
client_secret="sSG8Q~D1~uz-.xhfZ7vHyjyZIJmOTkBSM7CTxaPm"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dl.dfs.core.windows.net/circuits.csv")

# COMMAND ----------


