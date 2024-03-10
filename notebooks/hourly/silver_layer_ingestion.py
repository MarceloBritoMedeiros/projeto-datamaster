# Databricks notebook source
# MAGIC %md
# MAGIC #### 1) Importação das Bibliotecas

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Definição dos caminhos

# COMMAND ----------

bronze_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks/"
bronze_table = spark.read.format("delta").load(bronze_path)

silver_path = "dbfs:/mnt/stock_data/silver/yahoo_stocks/"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Tratamentos da Camada Silver

# COMMAND ----------

def get_most_recent_day(silver_path):
    """
    Cria uma tabela contendo as ações e suas respectivas informações. Aqui, cada ação vem como uma coluna e suas informações como subcolunas

    Args:
        tickers (list): lista de ações listadas na B3
        date (datetime): data da última atualização da base
    Returns:
        pandas.DataFrame: tabela contendo as ações e suas respectivas informações
    """
    
    try:
        silver_table = spark.read.format("delta").load(silver_path)
        return silver_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
    except:
        return datetime(2024,2,14)

# COMMAND ----------

def silver_transformation(bronze_path, silver_path):
    """
    Filtra a tabela bronze mais recente, e transforma as colunas, que até então são as ações em linhas e as subcolunas nas colunas principais

    Args:
        bronze_path (str): caminho da tabela bronze
        silver_path (str): caminho da tabela silver
    Returns:
        spark.DataFrame: tabela silver
    """

    day = get_most_recent_day(silver_path)
    bronze_table = spark.read.format("delta").load(bronze_path)
    ingest_table = bronze_table.filter(col("Date")>day)
    silver_df = ingest_table.groupby("Date", "Ticker").pivot("Price").sum("value")
    silver_df = silver_df.withColumnRenamed("Adj Close", "Adj_Close")
    silver_df2 = silver_df.filter((col("Adj_Close").isNotNull()))
    print("Transformação silver finalizada;")
    return silver_df2

# COMMAND ----------

def encrypt_data(plain_text, KEY):
    """
    Tem o objetivo de mascarar uma coluna utilizando o algoritmo AES (Advanced Encryption Standard), uma criptografia simétrica que utiliza chaves de 128 bits, 192 bits e 256 bits

    Args:
        encrypt_data (str): dado a ser criptografado
        KEY (str): chave de criptografia
    Returns:
        str: dado criptografado
    """
    f = Fernet(KEY)
    encrip_text = f.encrypt(str(plain_text).encode()).decode()
    return encrip_text

# COMMAND ----------

if __name__ == "__main__": 
    encrypt_udf = udf(encrypt_data, StringType())
    silver_table = silver_transformation(bronze_path, silver_path)
    encription_key = dbutils.secrets.get(scope="myblob", key="silver_key")
    silver_table_encripted = silver_table.withColumn("Ticker", encrypt_udf(col('Ticker'), lit(encription_key.encode('utf-8'))))
    silver_table_encripted.write.format("delta").partitionBy("Date").mode("append").save(silver_path)

# COMMAND ----------



# COMMAND ----------


