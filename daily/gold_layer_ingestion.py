# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col, date_format,hour, minute, second, monotonically_increasing_id, when, concat, lit
from datetime import datetime, timedelta
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet

# COMMAND ----------

silver_path = "dbfs:/mnt/stock_data/silver/yahoo_stocks_close/"
gold_path = "dbfs:/mnt/stock_data/gold/yahoo_stocks_close/"

# COMMAND ----------

def decrypt_data(encrypt_data, KEY):
    f = Fernet(bytes(KEY))
    decoded_val = f.decrypt(encrypt_data.encode()).decode()
    return decoded_val

# COMMAND ----------

def get_most_recent_day(gold_path):
    try:
        gold_table = spark.read.format("delta").load(gold_path)
        return gold_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
    except:
        return datetime(2024,2,19)

# COMMAND ----------

def previousDayTransformation(day, silver_path):
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

day = get_most_recent_day(gold_path)
gold_table1 = previousDayTransformation(day, silver_path)
gold_table2 = additionalColumns(gold_table1)
gold_table2.write.format("delta").partitionBy("Date").mode("append").save(gold_path)

# COMMAND ----------


