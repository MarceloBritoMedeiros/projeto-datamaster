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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, timedelta

bronze_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks/"

# COMMAND ----------

def ibov_stocks():
    """
    Retorna uma lista com os Tickers das ações listadas na B3

    Returns:
        list: lista de ações listadas na B3
    """
    br = inv.stocks.get_stocks(country='brazil')
    wallet = []
    for a in br['symbol']:
        if len(a) <= 5:
            wallet.append(a+'.SA')
    return wallet

# COMMAND ----------

def get_stocks(tickers, date):
    """
    Cria uma tabela contendo as ações e suas respectivas informações. Aqui, cada ação vem como uma coluna e suas informações como subcolunas

    Args:
        tickers (list): lista de ações listadas na B3
        date (datetime): data da última atualização da base
    Returns:
        pandas.DataFrame: tabela contendo as ações e suas respectivas informações
    """
    
    date_next = date + timedelta(hours=1)
    df = yf.download(tickers, start=date_next, interval='1h', ignore_tz=True)
    melted_df = pd.melt(df, value_vars=df.columns.tolist(), ignore_index=False).reset_index()
    return melted_df

# COMMAND ----------

def get_most_recent_day():
    """
    Retorna a data de atualização mais recente da tabela (última partição).

    Returns:
        datetime: data de atualização mais recente da tabela
    """
    try:
        bronze_table = spark.read.format("delta").load(bronze_path)
        return bronze_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
    except:
        return datetime(2024,2,14)

# COMMAND ----------

if __name__ == "__main__":
    day = get_most_recent_day()
    stocks_data = get_stocks(ibov_stocks(), day)
    stocks_df = spark.createDataFrame(stocks_data)
    stocks_df = stocks_df.withColumnRenamed("index", "Date")
    stocks_df.write.format("delta").partitionBy("Date").mode("append").save(bronze_path)

# COMMAND ----------


