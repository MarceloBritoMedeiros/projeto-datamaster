# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col, expr, udf, lit
from datetime import datetime, timedelta
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from cryptography.fernet import Fernet

# COMMAND ----------

# MAGIC %md
# MAGIC ### Camada Silver

# COMMAND ----------

bronze_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks_close/"
bronze_table = spark.read.format("delta").load(bronze_path)

silver_path = "dbfs:/mnt/stock_data/silver/yahoo_stocks_close/"

# COMMAND ----------

def get_most_recent_day(silver_path):
    try:
        silver_table = spark.read.format("delta").load(silver_path)
        return silver_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
    except:
        return datetime(2024,2,14)

# COMMAND ----------

def silver_transformation(bronze_path, silver_path):
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
    f = Fernet(KEY)
    encrip_text = f.encrypt(str(plain_text).encode()).decode()
    return encrip_text

encrypt_udf = udf(encrypt_data, StringType())

# COMMAND ----------

silver_table = silver_transformation(bronze_path, silver_path)
encription_key = dbutils.secrets.get(scope="myblob", key="silver_key")
silver_table_encripted = silver_table.withColumn("Ticker", encrypt_udf(col('Ticker'), lit(encription_key.encode('utf-8'))))

# COMMAND ----------


silver_table_encripted.write.format("delta").partitionBy("Date").mode("append").save(silver_path)

# COMMAND ----------

silver_path = "dbfs:/mnt/stock_data/silver/yahoo_stocks_close/"

# COMMAND ----------

bronze_table = spark.read.format("delta").load(silver_path)

# COMMAND ----------

display(bronze_table)

# COMMAND ----------


