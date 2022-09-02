#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, from_unixtime

def process_elt_bank_account():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"

    spark = SparkSession.builder.master("yarn").appName("Bank_Account_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    input_bank_account_df = spark.read.option("header", True)\
        .csv(f"gs://{bucket_input}/transaction_dataproc_dbn/resources/data/input/BANK_ACCOUNT.csv", sep=";")

    bank_account_df = input_bank_account_df\
        .select(
            trim(col("BANK_ACCOUNT_ID")).alias("bank_account_id"),
            trim(col("USER_ID")).alias("user_id"),
            trim(col("FRIENDLY_NAME")).alias("bank_description"),
            trim(col("IS_ACTIVE")).alias("bank_status_id"),
            from_unixtime(trim(col("CREATION_TIMESTAMP"))).alias("bank_timestamp")
        )

    bank_account_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.bank_account") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_elt_bank_account()
