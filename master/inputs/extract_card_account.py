#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, from_unixtime

def process_elt_card_account():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"

    spark = SparkSession.builder.master("yarn").appName('Card_Account_Table').getOrCreate()

    input_card_account_df = spark.read.option("header", True) \
        .csv(f"gs://{bucket_input}/transaction_dataproc_dbn/resources/data/input/CARD_ACCOUNT.csv", sep=";")

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    card_account_df = input_card_account_df \
        .select(
            trim(col("CARD_ACCOUNT_ID")).alias("card_account_id"),
            trim(col("USER_ID")).alias("user_id"),
            trim(col("CARD_TYPE")).alias("card_type"),
            trim(col("FRIENDLY_NAME")).alias("card_description"),
            trim(col("IS_ACTIVE")).alias("card_status_id"),
            from_unixtime(trim(col("CREATION_TIMESTAMP"))).alias("card_timestamp")
        )

    card_account_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.card_account") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_elt_card_account()
