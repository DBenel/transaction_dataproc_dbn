#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, from_unixtime

def process_elt_transaction():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"

    spark = SparkSession.builder.master("yarn").appName("Transaction_Table").getOrCreate()

    input_transaction_df = spark.read.option("header", True) \
        .csv(f"gs://{bucket_input}/transaction_dataproc_dbn/resources/data/input/TRANSACTION.csv", sep=";")

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    transaction_df = input_transaction_df \
        .filter(col("transaction_id").isNotNull()) \
        .select(
            trim(col("transaction_id")).alias("transaction_id"),
            trim(col("ACCOUNT_ID")).alias("account_id"),
            trim(col("TRANSACTION_TYPE")).alias("transaction_type"),
            trim(col("CURRENCY")).alias("currency_type"),
            trim(col("AMOUNT")).cast("double").alias("transaction_amount"),
            from_unixtime(trim(col("CREATION_TIMESTAMP"))).alias("transaction_timestamp")
        )

    transaction_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.transaction") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_elt_transaction()
