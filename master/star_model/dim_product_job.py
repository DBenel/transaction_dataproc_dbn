#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit, col, when, round

def select_product_card_df(card_df):
    product_card_df = card_df.select(
        col("card_account_id").alias("account_id"),
        col("card_type"),
        col("card_description").alias("product_description"),
        col("card_status_id").alias("product_status_id")
    )
    return product_card_df

def select_product_bank_df(bank_df):
    product_bank_df = bank_df.select(
        col("bank_account_id").alias("account_id"),
        lit(None).cast("string").alias("card_type"),
        col("bank_description").alias("product_description"),
        col("bank_status_id").alias("product_status_id")
    )
    return product_bank_df

def process_dim_product():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    data_set_star = sys.argv[4]
    bucket_gcs = f"{bucket_input}_tmp"

    spark = SparkSession.builder.master("yarn").appName("Dim_Product_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    card_account_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.card_account") \
        .load()
    bank_account_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.bank_account") \
        .load()

    product_card_df = select_product_card_df(card_account_df)
    product_bank_df = select_product_bank_df(bank_account_df)

    dim_product_df = product_card_df.union(product_bank_df)

    dim_product_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_star}.dim_product") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_dim_product()
