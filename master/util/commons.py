import os
import tempfile

import findspark
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col, count, create_map, lit, round, sum, when, trim, substring
from master.util.constants import Constants


class Commons:

    def __init__(self):
        self.constant = Constants()

    def df_trim_columns(self, df: DataFrame):
        column_list = list()
        for cl in df.dtypes:
            if cl[1] == "string":
                column_list.append(trim(col(cl[0])).alias(cl[0]))
            else:
                column_list.append(col(cl[0]))

        return df.select(*column_list)

    def multiple_cast_columns(self, df: DataFrame, cast_columns, type_cast):
        new_df = df.select(
            list(map(lambda name: col(name).cast(type_cast).alias(name) if name in cast_columns else col(name), df.columns))
        )
        return new_df

    def replace_miss_values(self, current_df, amount_columns):
        new_df = current_df\
            .na.fill("")\
            .na.fill(0, amount_columns)
        return new_df

    def select_df(self, df, column_list):
        return df.select(
            *list(map(lambda c: col(c), column_list))
        )

    def select_user_state_df(self, user_state_df):
        countries = self.constant.COUNTRIES
        mapping_expr = create_map([lit(x) for x in chain(*countries.items())])

        country_name = mapping_expr.getItem(col("country_id")).alias("country_name")
        user_df = user_state_df.select(
            col("user_id"),
            col("user_name"),
            country_name,
            col("user_state_type"),
        )
        return user_df

    def read_raw_csv_cs(self, spark, bucket_input, file_csv):
        path = f"gs://{bucket_input}{self.constant.PATH_CLOUD_STORAGE}{file_csv}.csv"
        new_df = spark.read.option("header", True) \
        .csv(path, sep=";")

        return new_df\
            .transform(lambda df: self.df_trim_columns(df))

    def read_bq_table_df(self, spark, path_table_name):
        big_query_df = spark.read.format("bigquery") \
            .option("table", f"{path_table_name}") \
            .load()

        return big_query_df

    def write_bq_df(self, output_df, path_table_name):
        output_df.write.format("bigquery") \
        .option("table", f"{path_table_name}") \
        .mode("append") \
        .save()
