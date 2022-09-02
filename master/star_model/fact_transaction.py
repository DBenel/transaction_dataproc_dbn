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

    new_t_df = transaction_df.select(
        col("transaction_id"),
        col("account_id"),
        date_id_column,
        col("currency_type"),
        col("transaction_type"),
        col("rate_amount"),
        col("transaction_amount").alias("foreign_currency_amount"),
        local_currency_amount_column,
        col("transaction_timestamp")
    )

    return new_t_df

def transaction_user_join(transaction_df, user_df):
    transaction_user_df = transaction_df.join(user_df,
        transaction_df.account_id == user_df.account_id, "left")\
        .drop(user_df.account_id)

    return  transaction_user_df \
        .select(col("transaction_id"),
        col("account_id"),
        col("user_id"),
        col("date_id"),
        col("transaction_type"),
        col("currency_type"),
        col("rate_amount"),
        col("foreign_currency_amount"),
        col("local_currency_amount"),
        col("transaction_timestamp"))

def select_user_id(card_df, bank_df):
    select_card_df = card_df.select(
        col("card_account_id").alias("account_id"),
        col("user_id"))

    select_bank_df = bank_df.select(
        col("bank_account_id").alias("account_id"),
        col("user_id"))

    return select_card_df.union(select_bank_df)

def process_fact_transaction():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    data_set_star = sys.argv[4]
    bucket_gcs = f"{bucket_input}_tmp"

    spark = SparkSession.builder.master("yarn").appName("Fact_Transaction_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    transaction_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.transaction") \
        .load()
    exchange_rate_df = spark.read \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.exchange_rate") \
        .load()

    card_account_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.card_account") \
        .load()
    bank_account_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.bank_account") \
        .load()

    DataFrame.transform = transform

    user_id_df = select_user_id(card_account_df, bank_account_df)

    fact_transaction_df = transaction_df\
        .transform(lambda df:  transaction_rate_join(df, exchange_rate_df))\
        .transform(lambda df:  select_transaction_currency(df)) \
        .transform(lambda df:  transaction_user_join(df, user_id_df))

    fact_transaction_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_star}.fact_transaction") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_fact_transaction()
