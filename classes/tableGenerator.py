import investpy as inv
import yfinance as yf
import pandas as pd
from pyspark.sql.functions import col, date_format,hour, minute, second, monotonically_increasing_id, when, concat, lit, expr, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, timedelta
from cryptography.fernet import Fernet

class TableGenerator:
    def __init__(self, spark):
        self.spark = spark

    def ibov_stocks(self):
        br = inv.stocks.get_stocks(country='brazil')
        wallet = []
        for a in br['symbol']:
            if len(a) <= 5:
                wallet.append(a+'.SA')
        return wallet
    
    def get_stocks(self, tickers, date):
        #date_str = date.strftime("%Y-%m-%d")
        date_next = date + timedelta(days=1)
        df = yf.download(tickers, start=date_next, interval='1d', ignore_tz=True)
        melted_df = pd.melt(df, value_vars=df.columns.tolist(), ignore_index=False).reset_index()
        return melted_df
    
    def get_most_recent_day(self):
        try:
            bronze_table = self.spark.read.format("delta").load(bronze_path)
            return bronze_table.select("Date").distinct().sort(col("Date").desc()).first()["Date"]
        except:
            return datetime(2024,2,14)
        
    def silver_transformation(self, bronze_path, silver_path):
        day = get_most_recent_day(silver_path)
        bronze_table = spark.read.format("delta").load(bronze_path)
        ingest_table = bronze_table.filter(col("Date")>day)
        silver_df = ingest_table.groupby("Date", "Ticker").pivot("Price").sum("value")
        silver_df = silver_df.withColumnRenamed("Adj Close", "Adj_Close")
        silver_df2 = silver_df.filter((col("Adj_Close").isNotNull()))
        print("Transformação silver finalizada;")
        return silver_df2

    def encrypt_data(self, plain_text, KEY):
        f = Fernet(KEY)
        encrip_text = f.encrypt(str(plain_text).encode()).decode()
        return encrip_text

    def decrypt_data(self, encrypt_data, KEY):
        f = Fernet(bytes(KEY))
        decoded_val = f.decrypt(encrypt_data.encode()).decode()
        return decoded_val

    def previousDayTransformation(self, day, silver_path):
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
    
    def additionalColumns(self, silver_table3):
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
    
    

