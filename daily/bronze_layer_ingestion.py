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

bronze_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks_close/"
info_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks_info/"

# COMMAND ----------

def ibov_stocks():
    br = inv.stocks.get_stocks(country='brazil')
    wallet = []
    for a in br['symbol']:
        if len(a) <= 5:
            wallet.append(a+'.SA')
    return wallet

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extraindo Informações Auxiliares da Empresa

# COMMAND ----------

def company_table(ticker):
    company_dict = yf.Ticker(ticker).info
    fields = []
    for i in company_dict.keys():
        fields.append(StructField(i, StringType(), nullable=True))

    schema=StructType(fields)
    
    company_df = spark.createDataFrame([company_dict], schema=schema)
    try:
        company_df_select = company_df.select('address1', 'address2', 'city', 'state', 'zip', 'country', 'phone', 'website',
                                'industry', 'industryKey', 'industryDisp', 'sector', 'sectorKey', 'sectorDisp',
                                'longBusinessSummary','symbol','underlyingSymbol','shortName',
                                'longName')
        return company_df_select
    except:
        return 0    

def company_data(tickers):
    company_df_select_full = company_table(tickers[0])    
    for i in tickers[1:]:
        table = company_table(i)
        if table!=0:
            company_df_select_full = company_df_select_full.unionByName(table)     
    return company_df_select_full

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

if __name__ == "__main__":
    day = get_most_recent_day()
    stocks_data = get_stocks(ibov_stocks(), day)
    stocks_df = spark.createDataFrame(stocks_data)
    stocks_df = stocks_df.withColumnRenamed("index", "Date")
    info_df = company_data(ibov_stocks())    
    stocks_df.write.format("delta").partitionBy("Date").mode("append").save(bronze_path)
    info_df.write.format("delta").partitionBy("Date").mode("overwrite").save(info_path)

# COMMAND ----------


