# Databricks notebook source
!pip install investpy
!pip install yfinance

# COMMAND ----------

import investpy as inv
import yfinance as yf
import pandas as pd
from pyspark.sql.functions import col
from datetime import datetime, timedelta

# COMMAND ----------

def ibov_stocks():
    br = inv.stocks.get_stocks(country='brazil')
    wallet = []
    for a in br['symbol']:
        if len(a) <= 5:
            wallet.append(a+'.SA')
    return wallet

# COMMAND ----------

def get_stocks(tickers, date):
    #date_str = date.strftime("%Y-%m-%d")
    date_next = date + timedelta(hours=1)
    df = yf.download(tickers, start=date_next, interval='1h', ignore_tz=True)
    melted_df = pd.melt(df, value_vars=df.columns.tolist(), ignore_index=False).reset_index()
    return melted_df

# COMMAND ----------

bronze_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks/"
bronze_table = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

particoes = bronze_table.select("Date").distinct().sort(col("Date").desc()).first()

# COMMAND ----------

particoes

# COMMAND ----------

display(bronze_table)

# COMMAND ----------

pandas_df = get_stocks(ibov_stocks(), particoes["Date"])
spark_df = spark.createDataFrame(pandas_df)
spark_df = spark_df.withColumnRenamed("index", "Date")

# COMMAND ----------

path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks/"

spark_df.write.format("delta").partitionBy("Date").mode("append").save(path)

# COMMAND ----------


