# Databricks notebook source
dbutils.fs.ls("/mnt")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "d01f51ef-13f7-4ffc-92cb-b34f0c418a96",
          "fs.azure.account.oauth2.client.secret": "q9D8Q~GhDQarrP-2iZL_~qDvnSQ842NTad9.tbnJ",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/342cb166-1398-4323-91ac-d401e02b10cf/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://stocks@datalakedatamaster.dfs.core.windows.net/",
  mount_point = "/mnt/stock_data",
  extra_configs = configs)

# COMMAND ----------


