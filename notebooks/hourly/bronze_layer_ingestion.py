# Databricks notebook source
# MAGIC %md
# MAGIC #### 1) Instalação das dependências

# COMMAND ----------

!pip install investpy
!pip install yfinance
!pip install holidays

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
import holidays

bronze_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks/"

# COMMAND ----------

def is_business_day(date):
    """
    Informa se o dia informado é dia útil ou não
    Args:
        date (datetime): dia a ser verificado
    Returns:
        boolean: se é dia útil ou não
    """
    # Verifica se é um dia da semana (segunda a sexta-feira)
    if date.weekday() < 5:
        # Verifica se não é um feriado
        feriados = holidays.country_holidays("BR")
        if not date in feriados[f"{datetime.now().year}-01-01":f"{datetime.now().year}-12-31"]:
            if date.hour>=12 or date.hour<=21:
                return True
    return False

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
    Cria uma tabela contendo as ações e suas respectivas informações. Aqui, os rótulos de algumas colunas estão contidos na coluna Price

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
        return datetime.now()

# COMMAND ----------

logs = []
if __name__ == "__main__":
    if is_business_day(datetime.now()):
        try:
            day = get_most_recent_day()
            message = ((f"Dia mais recente: {day}, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",))
            logs.append(message)
            print(message)

            stocks_data = get_stocks(ibov_stocks(), day)
            message = ((f"Ações extraidas, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",))
            logs.append(message)
            print(message)

            stocks_df = spark.createDataFrame(stocks_data)
            message = ((f"Dataframe criado, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",))
            logs.append(message)
            print(message)

            stocks_df = stocks_df.withColumnRenamed("index", "Date")
            stocks_df.write.format("delta").partitionBy("Date").mode("append").save(bronze_path)
            message = ((f"Tabela salva, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",))
            logs.append(message)
            print(message)

        except Exception as e:
            message = (f"{e}, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",)
            logs.append(message)
            print(message)
        logs_df = spark.createDataFrame(logs, ["Log"])
        logs_df.write.mode("append").text("dbfs:/mnt/stock_data/bronze/bronze_log")

# COMMAND ----------


