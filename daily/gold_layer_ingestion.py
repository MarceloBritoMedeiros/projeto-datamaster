# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col, date_format,hour, minute, second, monotonically_increasing_id, when
from datetime import datetime, timedelta

# COMMAND ----------

silver_path = "dbfs:/mnt/stock_data/silver/yahoo_stocks_close/"
silver_table = spark.read.format("delta").load(silver_path)

gold_path = "dbfs:/mnt/stock_data/gold/yahoo_stocks_close/"


# COMMAND ----------

gold_table2 = gold_table.withColumn("Date2", date_format("Date", "dd/MM/yyyy"))

# COMMAND ----------

def get_most_recent_day():
    try:
        gold_table = spark.read.format("delta").load(gold_path)
        return gold_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
    except:
        return datetime(2024,2,19)

# COMMAND ----------

day = get_most_recent_day()
ingest_table = silver_table.filter(col("Date")>day)

# COMMAND ----------

silver_table = silver_table.withColumn("Dia", date_format(col("Date"), "yyyy-MM-dd"))

# COMMAND ----------

silver_table2 = silver_table.withColumnRenamed("Adj_Close", "Adj_Close_ant")

# COMMAND ----------

days = silver_table2.select("Dia").distinct().sort(col("Dia")).withColumn("Index", monotonically_increasing_id()).withColumn("Index2", col("Index")+1)
day1 = days.select(col("Dia"), col("Index"))
day2 = days.select(col("Dia").alias("Dia2"), col("Index2"))
depara_days = day1.join(day2, day1.Index==day2.Index2, "inner").select(col("Dia"), col("Dia2"))

# COMMAND ----------

silver_table = silver_table.join(depara_days, 
                                 silver_table.Dia==depara_days.Dia, 
                                 "left")\
                            .select(silver_table["*"], 
                                    depara_days["Dia2"])\
                            .drop("Dia")

# COMMAND ----------

silver_table3 = silver_table.join(silver_table2, 
                                  (silver_table.Ticker == silver_table2.Ticker) & 
                                  (silver_table.Dia2 == silver_table2.Dia), 
                                  "left") \
                           .select(silver_table["*"], 
                                   silver_table2["Adj_Close_ant"])

# COMMAND ----------

### Crie uma outra tabela filtrada com as datas de 16 horas e crie uma coluna com o dia seguinte, que será usada de chave de cruzemento

# COMMAND ----------

spark_df = silver_table3.withColumn("Marketcap", col("Adj_Close")*col("Volume"))\
    .withColumn("Volatility", col("Adj_Close")-col("Adj_Close_ant"))\
        .withColumn("Volatility_perc", (col("Adj_Close")/col("Adj_Close_ant"))-1)\
            .withColumn("Volatility_label", when(col("Volatility")>0, "Increasing")\
                .when(col("Volatility")==0, "Stable")\
                    .otherwise("Decreasing"))

# COMMAND ----------

spark_df = spark_df.withColumn("Date2", date_format("Date", "dd/MM/yyyy"))

# COMMAND ----------

gold_path = "dbfs:/mnt/stock_data/gold/yahoo_stocks_close/"
spark_df.write.format("delta").partitionBy("Date").mode("append").save(gold_path)

# COMMAND ----------

display()
