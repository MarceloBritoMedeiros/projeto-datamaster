# Databricks notebook source
# MAGIC %md
# MAGIC #### 1) Importação das Bibliotecas

# COMMAND ----------

!pip install holidays
import holidays
import pandas as pd
from pyspark.sql.functions import col, expr, udf, lit
from datetime import datetime, timedelta
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from cryptography.fernet import Fernet

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

def get_most_recent_day(silver_path):
    """
    Cria uma tabela contendo as ações e suas respectivas informações. Aqui, os rótulos de algumas colunas estão contidos na coluna Price

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

logs = []
if __name__ == "__main__":
    if is_business_day(datetime.now()):
        try:
            encrypt_udf = udf(encrypt_data, StringType())
            silver_table = silver_transformation(bronze_path, silver_path)
            logs.append(((f"Tabela silver transformada, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",)))
            encription_key = "IUlmIR12doubSltXKyIyGgJcWFgcmx8OXKD4LUMAOE0="#dbutils.secrets.get(scope="myblob", key="silver_key")
            silver_table_encripted = silver_table.withColumn("Ticker", encrypt_udf(col('Ticker'), lit(encription_key.encode('utf-8'))))
            logs.append(((f"Coluna Ticker criptografada, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",)))
            silver_table_encripted.write.format("delta").partitionBy("Date").mode("append").save(silver_path)
            logs.append(((f"Tabela salva, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",)))
        except Exception as e:
            logs.append((f"{e}, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",))
        logs_df = spark.createDataFrame(logs, ["Log"])
        logs_df.write.mode("append").text("dbfs:/mnt/stock_data/silver/silver_log")

# COMMAND ----------


