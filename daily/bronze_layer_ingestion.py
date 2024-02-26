# Databricks notebook source
# MAGIC %md
# MAGIC #### 1) Instalação das dependências

# COMMAND ----------

!pip install investpy
!pip install yfinance

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Importação das bibliotecas

# COMMAND ----------

import investpy as inv
import yfinance as yf
import pandas as pd
from pyspark.sql.functions import col
from datetime import datetime, timedelta

bronze_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks_close/"

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
    date_next = date + timedelta(days=1)
    df = yf.download(tickers, start=date_next, interval='1d', ignore_tz=True)
    melted_df = pd.melt(df, value_vars=df.columns.tolist(), ignore_index=False).reset_index()
    return melted_df

# COMMAND ----------

def get_most_recent_day():
    try:
        bronze_table = spark.read.format("delta").load(bronze_path)
        return bronze_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
    except:
        return datetime(2024,2,14)

# COMMAND ----------

def main():
    day = get_most_recent_day()
    pandas_df = get_stocks(ibov_stocks(), day)
    spark_df = spark.createDataFrame(pandas_df)
    spark_df = spark_df.withColumnRenamed("index", "Date")
    spark_df.write.format("delta").partitionBy("Date").mode("overwrite").save(bronze_path)

# COMMAND ----------

main()

# COMMAND ----------


