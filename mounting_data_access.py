# Databricks notebook source
dbutils.fs.ls("/mnt")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="myblob", key="aplication_id"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="myblob", key="service-credential-key-name"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='myblob', key='directory-id')}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://stocks@datalakedatamaster.dfs.core.windows.net/",
  mount_point = "/mnt/stock_data",
  extra_configs = configs)

# COMMAND ----------


