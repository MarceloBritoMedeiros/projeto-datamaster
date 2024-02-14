# Databricks notebook source
!pip install investpy
!pip install yfinance

# COMMAND ----------

import investpy as inv
import yfinance as yf
import pandas as pd

# COMMAND ----------

def ibov_stocks():
    br = inv.stocks.get_stocks(country='brazil')
    wallet = []
    for a in br['symbol']:
        if len(a) <= 5:
            wallet.append(a+'.SA')
    return wallet

# COMMAND ----------

df = yf.download(ibov_stocks(), start='2024-01-30', interval='1h', ignore_tz=True)

# COMMAND ----------

melted_df = pd.melt(df, value_vars=df.columns.tolist(), ignore_index=False).reset_index()

# COMMAND ----------

spark_df = spark.createDataFrame(melted_df)

# COMMAND ----------

spark_df = spark_df.withColumnRenamed("index", "Date")

# COMMAND ----------

display(spark_df)

# COMMAND ----------

path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks/"

spark_df.write.format("delta").partitionBy("Date").mode("overwrite").save(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Camada Silver

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

# COMMAND ----------

bronze_path = "dbfs:/mnt/dados/bronze/yahoo_stocks/"
bronze_table = spark.read.format("delta").load(path)

# COMMAND ----------

display(bronze_table)

# COMMAND ----------

spark_df = bronze_table.filter(col("value").isNotNull())

# COMMAND ----------

spark_df.count()

# COMMAND ----------

spark_df2.count()

# COMMAND ----------

bronze_table.count()

# COMMAND ----------

display(spark_df)

# COMMAND ----------

spark_df = bronze_table.groupby("Date", "Ticker").pivot("Price").sum("value")

# COMMAND ----------

spark_df = spark_df.withColumnRenamed("Adj Close", "Adj_Close")

# COMMAND ----------

spark_df2 = spark_df.filter((col("Adj_Close").isNotNull()))

# COMMAND ----------

display(spark_df2)

# COMMAND ----------

particoes = spark_df2.select("Date").distinct().sort(col("Date").desc()).first()

# COMMAND ----------

spark_df2.filter((col("Adj_Close").isNull())).show()

# COMMAND ----------

path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks/"

spark_df2.write.format("delta").partitionBy("Date").mode("overwrite").save(path)

# COMMAND ----------


