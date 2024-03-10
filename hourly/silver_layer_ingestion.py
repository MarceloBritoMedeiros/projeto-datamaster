# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Camada Silver

# COMMAND ----------

bronze_path = "dbfs:/mnt/stock_data/bronze/yahoo_stocks/"
bronze_table = spark.read.format("delta").load(bronze_path)

silver_path = "dbfs:/mnt/stock_data/silver/yahoo_stocks/"

# COMMAND ----------

def get_most_recent_day(silver_path):
    try:
        silver_table = spark.read.format("delta").load(silver_path)
        return silver_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
    except:
        return datetime(2024,2,14)

# COMMAND ----------

#bronze_table.count()

# COMMAND ----------

#spark_df.count()

# COMMAND ----------

#spark_df2.count()

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

def info_transformation(info_bronze_path):
    info_table = spark.read.format("delta").load(info_bronze_path)
    silver_info_table = info_table.distinct() 
    print("Tranformação de tabela informacional finalizada")
    return silver_info_table

# COMMAND ----------

def save(df, silver_path):    
    print(f"Tabela salva em {silver_path}")

# COMMAND ----------

silver_table = silver_transformation(bronze_path, silver_path)
silver_table.write.format("delta").partitionBy("Date").mode("append").save(silver_path)

# COMMAND ----------



# COMMAND ----------


