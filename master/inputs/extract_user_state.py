#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, from_unixtime

def process_elt_user_state():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"

    spark = SparkSession.builder.master("yarn").appName("User_Table").getOrCreate()

    input_user_state_df = spark.read.option("header", True) \
        .csv(f"gs://{bucket_input}/transaction_dataproc_dbn/resources/data/input/USER_STATE.csv", sep=";")

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    user_state_df = input_user_state_df \
        .select(
            trim(col("USER_STATE_ID")).alias("user_state_id"),
            trim(col("USER_STATE")).alias("user_state_type")
        )

    user_state_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.user_state") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    process_elt_user_state()
