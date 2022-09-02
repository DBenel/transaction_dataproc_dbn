#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit, col, when, round

def transform(self, f):
    return f(self)

def user_state_join(user_df, state_df):
    return user_df.join(state_df, user_df.user_state_id == state_df.user_state_id, "left")\
        .drop(user_df.user_state_id).drop(state_df.user_state_id)

def match_status_column(name):
    if (name == "card_status_id") or (name == "bank_status_id"):
        return when(col(name) == "0", lit("NOT ACTIVE")).otherwise(lit("ACTIVE")).alias(name)
    else:
        return col(name)

def user_product_join(user_df, product_df):
    new_df = user_df.join(product_df, user_df.user_id == product_df.user_id, "left") \
        .drop(product_df.user_id)
    return new_df.select(
        list(map(lambda c: match_status_column(c), new_df.columns))
    )

def user_card_transaction(user_card_df,transaction_df):
    new_user_card_df = user_card_df \
        .join(
            transaction_df,
            user_card_df.card_account_id == transaction_df.account_id,
            "left"
        ) \
        .drop(transaction_df.account_id)
    return new_user_card_df

def user_bank_transaction(user_bank_df,transaction_df):
    new_user_bank_df = user_bank_df \
        .join(
            transaction_df,
            user_bank_df.bank_account_id == transaction_df.account_id,
            "left"
        ) \
        .drop(transaction_df.account_id)
    return new_user_bank_df

def select_user_card_df(user_card_df):
    new_user_card_df = user_card_df.select(
        col("user_id"),
        col("transaction_id"),
        col("country_id"),
        col("card_account_id").alias("account_id"),
        col("date_id"),
        col("user_name"),
        col("user_state_type"),
        col("card_type"),
        col("card_description").alias("product_description"),
        col("card_status_id").alias("product_status_id"),
        col("transaction_type"),
        col("currency_type"),
        col("rate_amount"),
        col("foreign_currency_amount"),
        col("local_currency_amount"),
        col("month_id"),
        col("hour_id"),
        col("transaction_timestamp")
    )
    return new_user_card_df

def select_user_bank_df(user_bank_df):
    new_user_bank_df = user_bank_df.select(
        col("user_id"),
        col("transaction_id"),
        col("country_id"),
        col("bank_account_id").alias("account_id"),
        col("date_id"),
        col("user_name"),
        col("user_state_type"),
        lit(None).cast("string").alias("card_type"),
        col("bank_description").alias("product_description"),
        col("bank_status_id").alias("product_status_id"),
        col("transaction_type"),
        col("currency_type"),
        col("rate_amount"),
        col("foreign_currency_amount"),
        col("local_currency_amount"),
        col("month_id"),
        col("hour_id"),
        col("transaction_timestamp")
    )
    return new_user_bank_df

def process_transaction_master():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"

    spark = SparkSession.builder.master("yarn").appName("Transaction_Master_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    users_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.user") \
        .load()
    user_state_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.user_state") \
        .load()

    p_card_account_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.card_account") \
        .load()
    p_bank_account_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.bank_account") \
        .load()
    transaction_currency_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.transaction_currency") \
        .load()

    DataFrame.transform = transform

    users_new_df = users_df \
        .transform(lambda df:  user_state_join(df, user_state_df))

    user_card_df = users_new_df \
        .transform(lambda df: user_product_join(df, p_card_account_df))\
        .transform(lambda df: user_card_transaction(df, transaction_currency_df))\
        .transform(lambda df: select_user_card_df(df))

    user_bank_df = users_new_df \
        .transform(lambda df: user_product_join(df, p_bank_account_df))\
        .transform(lambda df: user_bank_transaction(df, transaction_currency_df))\
        .transform(lambda df: select_user_bank_df(df))

    master_df = user_card_df.union(user_bank_df)

    master_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_master}.transaction_master") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_transaction_master()
