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
silver_table = spark.read.format("delta").load(silver_path)

# COMMAND ----------

particoes = silver_table.select("Date").distinct().sort(col("Date").desc()).first()
ingest_table = bronze_table.filter(col("Date")>particoes["Date"])

# COMMAND ----------



# COMMAND ----------

display(ingest_table.select("Date").distinct())

# COMMAND ----------

#bronze_table.count()

# COMMAND ----------

#spark_df.count()

# COMMAND ----------

#spark_df2.count()

# COMMAND ----------

spark_df = ingest_table.groupby("Date", "Ticker").pivot("Price").sum("value")

# COMMAND ----------

spark_df = spark_df.withColumnRenamed("Adj Close", "Adj_Close")

# COMMAND ----------

spark_df2 = spark_df.filter((col("Adj_Close").isNotNull()))

# COMMAND ----------

display(spark_df2)

# COMMAND ----------

spark_df2.write.format("delta").partitionBy("Date").mode("append").save(silver_path)

# COMMAND ----------


