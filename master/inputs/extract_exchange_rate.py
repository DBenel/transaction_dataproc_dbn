#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

def process_elt_exchange_rate():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    spark = SparkSession.builder.master("yarn").appName("Exchange_Rate_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    input_exchange_rate_df = spark.read.option("header", True) \
        .csv(f"gs://{bucket_input}/transaction_dataproc_dbn/resources/data/input/EXCHANGE_RATE.csv", sep=";")

    exchange_rate_df = input_exchange_rate_df\
        .select(
            trim(col("DATE_ID")).alias("date_id"),
            trim(col("CURRENCY")).alias("currency_type"),
            trim(col("RATE_AMOUNT")).cast("double").alias("rate_amount")
        )

    exchange_rate_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.exchange_rate") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    process_elt_exchange_rate()