#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit, concat, col, when, substring, round

def transform(self, f):
    return f(self)

def transaction_rate_join(transaction_df, exchange_rate_df):
    new_join_df = transaction_df \
        .join(
            exchange_rate_df,
            ((substring(transaction_df.transaction_timestamp, 0, 10) == exchange_rate_df.date_id) &
             (transaction_df.currency_type == exchange_rate_df.currency_type)),
            "left"
        ).drop(exchange_rate_df.currency_type)

    return new_join_df

def select_transaction_currency(transaction_df):
    local_currency_amount_column = when(
        ((col("rate_amount").isNull()) & (col("currency_type") == lit("EUR"))), col("transaction_amount")
    ).otherwise(
        round(col("transaction_amount") * col("rate_amount"), 2)
    ).alias("local_currency_amount")

    date_id_column = when(col("date_id").isNotNull(), col("date_id"))\
        .otherwise(substring(col("transaction_timestamp"), 0, 10))\
        .cast("date").alias("date_id")
    month_id_column = concat(
        substring(col("transaction_timestamp"), 0, 4),
        substring(col("transaction_timestamp"), 6, 2)
    ).alias("month_id")

    new_t_df = transaction_df.select(
        col("transaction_id"),
        col("account_id"),
        col("transaction_type"),
        col("currency_type"),
        col("rate_amount"),
        col("transaction_amount").alias("foreign_currency_amount"),
        local_currency_amount_column,
        date_id_column,
        month_id_column,
        substring(col("transaction_timestamp"), 12, 5).alias("hour_id"),
        col("transaction_timestamp")
    )

    return new_t_df

def process_transaction_currency():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    spark = SparkSession.builder.master("yarn").appName("Transaction_Currency_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    transaction_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.transaction") \
        .load()
    exchange_rate_df = spark.read \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.exchange_rate") \
        .load()

    DataFrame.transform = transform

    transaction_currency_df = transaction_df\
        .transform(lambda df:  transaction_rate_join(df, exchange_rate_df))\
        .transform(lambda df:  select_transaction_currency(df))

    transaction_currency_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.transaction_currency") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_transaction_currency()
