#!/usr/bin/env python

import sys
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, create_map, lit, round, when

def transform(self, f):
    return f(self)

def user_state_join(user_df, state_df):
    return user_df.join(state_df, user_df.user_state_id == state_df.user_state_id, "left")\
        .drop(user_df.user_state_id).drop(state_df.user_state_id)

def select_user_state_df(user_state_df):
    countries = {
        "DE": "Germany",
        "ES": "Spain",
        "MT": "Malta",
        "EN": "United Kingdom"
    }
    mapping_expr = create_map([lit(x) for x in chain(*countries.items())])

    country_name = mapping_expr.getItem(col("country_id")).alias("country_name")
    dim_user_df = user_state_df.select(
        col("user_id"),
        col("user_name"),
        country_name,
        col("user_state_type"),
    )
    return dim_user_df

def process_dim_user():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    data_set_star = sys.argv[4]
    bucket_gcs = f"{bucket_input}_tmp"

    spark = SparkSession.builder.master("yarn").appName("Dim_User_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    users_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.user") \
        .load()
    user_state_df = spark.read.format('bigquery') \
        .option("table", f"{project_id}.{data_set_master}.user_state") \
        .load()

    DataFrame.transform = transform

    dim_user_df = users_df \
        .transform(lambda df:  user_state_join(df, user_state_df)) \
        .transform(lambda df:  select_user_state_df(df))

    dim_user_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{data_set_star}.dim_user") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_dim_user()
