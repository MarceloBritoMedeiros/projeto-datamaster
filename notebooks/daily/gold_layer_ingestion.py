# Databricks notebook source
# MAGIC %md
# MAGIC #### 1) Importação das Bibliotecas

# COMMAND ----------

!pip install holidays
import holidays
import pandas as pd
from pyspark.sql.functions import col, date_format,hour, minute, second, monotonically_increasing_id, when, concat, lit
from datetime import datetime, timedelta
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Definição dos caminhos

# COMMAND ----------

silver_path = "dbfs:/mnt/stock_data/silver/yahoo_stocks_close/"
gold_path = "dbfs:/mnt/stock_data/gold/yahoo_stocks_close/"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Tratamentos da Camada Gold

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
            return True
    return False

# COMMAND ----------

def decrypt_data(encrypt_data, KEY):
    """
    Tem o objetivo de descriptografar uma coluna mediante o padrão AES (Advanced Encryption Standard)

    Args:
        encrypt_data (str): dado a ser descriptografado
        KEY (str): chave de criptografia
    Returns:
        str: dado descriptografado
    """
    f = Fernet(bytes(KEY))
    decoded_val = f.decrypt(encrypt_data.encode()).decode()
    return decoded_val

# COMMAND ----------

def get_most_recent_day(gold_path):
    """
    Cria uma tabela contendo as ações e suas respectivas informações. Aqui, cada ação vem como uma coluna e suas informações como subcolunas

    Args:
        tickers (list): lista de ações listadas na B3
        date (datetime): data da última atualização da base
    Returns:
        pandas.DataFrame: tabela contendo as ações e suas respectivas informações
    """
    try:
        gold_table = spark.read.format("delta").load(gold_path)
        return gold_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
    except:
        return datetime(2024,2,19)

# COMMAND ----------

def previousDayTransformation(day, silver_path):
    """
    Filtra a tabela silver mais recente, aplica a função de descriptografia e cria uma coluna com os dados de fechamento do dia anterior para cada ação por meio de self joins

    Args:
        day (datetime): data da última atualização da base
        silver_path (str): caminho da tabela silver
    Returns:
        spark.DataFrame: tabela contendo os tratamentos listados
    """

    silver_table_encripted = spark.read.format("delta").load(silver_path).filter(col("Date")>day)
    decrypt_udf = udf(decrypt_data, StringType())
    encription_key = dbutils.secrets.get(scope="myblob", key="silver_key")
    
    silver_table = silver_table_encripted.withColumn("Ticker_decripted", decrypt_udf(col('Ticker'), lit(encription_key.encode('utf-8'))))
    silver_table = silver_table.drop("Ticker").withColumnRenamed("Ticker_decripted", "Ticker")
    
    silver_table = silver_table.withColumn("Dia", date_format(col("Date"), "yyyy-MM-dd"))
    silver_table2 = silver_table.withColumnRenamed("Adj_Close", "Adj_Close_ant")

    days = silver_table2.select("Dia").distinct().sort(col("Dia")).withColumn("Index", monotonically_increasing_id()).withColumn("Index2", col("Index")+1)
    day1 = days.select(col("Dia"), col("Index"))
    day2 = days.select(col("Dia").alias("Dia2"), col("Index2"))
    depara_days = day1.join(day2, day1.Index==day2.Index2, "inner").select(col("Dia"), col("Dia2"))
    silver_table = silver_table.join(depara_days, 
                                 silver_table.Dia==depara_days.Dia, 
                                 "left")\
                            .select(silver_table["*"], 
                                    depara_days["Dia2"])\
                            .drop("Dia")
    silver_table3 = silver_table.join(silver_table2, 
                                  (silver_table.Ticker == silver_table2.Ticker) & 
                                  (silver_table.Dia2 == silver_table2.Dia), 
                                  "left") \
                           .select(silver_table["*"], 
                                   silver_table2["Adj_Close_ant"])
    return silver_table3

# COMMAND ----------

def additionalColumns(silver_table3):
    """
    Cria algumas colunas calculadas a fim de facilitar o consumo da informações

    Args:
        silver_table3 (spark.DataFrame): tabela silver
    Returns:
        spark.DataFrame: uma tabela com as colunas adicionadas
    """

    gold_df = silver_table3.withColumn("Marketcap", col("Adj_Close")*col("Volume"))\
    .withColumn("Volatility", col("Adj_Close")-col("Adj_Close_ant"))\
        .withColumn("Volatility_perc", (col("Adj_Close")/col("Adj_Close_ant"))-1)\
            .withColumn("Volatility_label", when(col("Volatility")>0, "Increasing")\
                .when(col("Volatility")==0, "Stable")\
                    .otherwise("Decreasing"))
    gold_df = gold_df.withColumn("Date2", date_format("Date", "dd/MM/yyyy"))\
        .withColumn("Date2", date_format("Date", "dd/MM/yyyy"))
    gold_df = gold_df.withColumn("key", concat(col("Date2"), col("Ticker")))  
    return gold_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) Execução da pipeline

# COMMAND ----------

logs = []
if __name__ == "__main__": 
    if is_business_day(datetime.now()):
        try:
            day = get_most_recent_day(gold_path)
            logs.append(((f"Dia mais recente: {day}, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",)))
            gold_table1 = previousDayTransformation(day, silver_path)
            gold_table2 = additionalColumns(gold_table1)
            logs.append(((f"Tranformações gold aplicadas, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",)))
            gold_table2.write.format("delta").partitionBy("Date").mode("append").save(gold_path)
            logs.append(((f"Tabela salva, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",)))
        except Exception as e:
            logs.append((f"{e}, datetime.now().strftime('%Y-%m-%d %H:%M:%S')",))
        logs_df = spark.createDataFrame(logs, ["Log"])
        logs_df.write.mode("append").text("dbfs:/mnt/stock_data/gold/gold_close_log")

# COMMAND ----------


