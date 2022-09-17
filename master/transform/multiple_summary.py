#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, round, sum, when
from pyspark.sql.window import Window
from master.util.commons import Commons
from master.util.constants import Constants

class MultipleSummary:

    def __init__(self):
        self.common = Commons()

    def read_txn_table_df(self, spark, project_id, data_set_master, bank_table_bq):
        table_path = f"{project_id}.{data_set_master}.{bank_table_bq}"
        new_df = self.common.read_bq_table_df(spark, table_path)

        return new_df

    def write_txn_bq_df(self, bank_output_df, project_id, data_set_master, bank_table_bq):
        user_table_path = f"{project_id}.{data_set_master}.{bank_table_bq}"
        self.common.write_bq_df(bank_output_df, user_table_path)

    def transaction_user_agg(self, transaction_df):
        transaction_user_agg_df = transaction_df\
            .filter(col("transaction_id").isNotNull())\
            .groupBy(col("user_name"))\
            .agg(
                sum(when(
                    col("user_name") == "deposit", round(col("local_currency_amount"), 2)
                ).otherwise(lit(0))).alias("total_eur_deposit_amount"),
                sum(when(
                    col("user_name") == "settlement", round(col("local_currency_amount"), 2)
                ).otherwise(lit(0))).alias("total_eur_settlement_amount")
            )
        return transaction_user_agg_df.select(
            col("user_name"),
            col("total_eur_deposit_amount"),
            col("total_eur_settlement_amount"),
            round(col("total_eur_deposit_amount")+col("total_eur_settlement_amount"), 2).alias("total_eur_amount")
        )


    def transaction_cp_agg(self, transaction_df, column_name):
        new_t_df = transaction_df\
            .filter(col("transaction_id").isNotNull())\
            .groupBy(col(column_name), col("transaction_type"), col("currency_type"))\
            .agg(
                count(col("transaction_id")).cast("int").alias("count_transactions"),
                sum(col("foreign_currency_amount")).alias("foreign_currency_amount"),
                sum(col("local_currency_amount")).alias("local_currency_amount")
            ).orderBy(col(column_name))

        return new_t_df


    def transaction_country_window(self, transaction_df):
        window_spec_agg = Window.partitionBy(col("country_id"), col("transaction_type"))
        sum_amount_column = sum(col("local_currency_amount")).over(window_spec_agg).alias("total_amount")
        count_currency_column = count(col("transaction_type")).over(window_spec_agg).cast("int").alias("count_currency")
        percentage_column = round((col("local_currency_amount")/col("total_amount"))*lit(100), 2) \
            .alias("percentage_country_type")

        transaction_w_df = transaction_df.select(
            *list(map(lambda c: col(c), transaction_df.columns)), sum_amount_column, count_currency_column
        )
        return transaction_w_df.select(
            *list(map(lambda c: col(c), transaction_w_df.columns)), percentage_column
        )


def flow_process_multiple_summary():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    txn_master_table_bq = Constants().TXN_MASTER_TABLE_BQ
    summary_user_table_bq = Constants().SUMMARY_USER_TABLE_BQ
    summary_country_table_bq = Constants().SUMMARY_COUNTRY_TABLE_BQ
    summary_product_table_bq = Constants().SUMMARY_PRODUCT_TABLE_BQ
    multi_summary = MultipleSummary()

    spark = SparkSession.builder.master("yarn").appName("Multiple_Summary_Tables").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    transaction_master_df = multi_summary.read_txn_table_df(spark, project_id, data_set_master, txn_master_table_bq)
    
    transaction_user_agg_df = transaction_master_df \
        .transform(lambda df:  multi_summary.transaction_user_agg(df))
    
    transaction_country_agg_df = transaction_master_df \
        .transform(lambda df:  multi_summary.transaction_cp_agg(df, "country_id")) \
        .transform(lambda df:  multi_summary.transaction_country_window(df))
    
    transaction_product_agg_df = transaction_master_df \
        .transform(lambda df:  multi_summary.transaction_cp_agg(df, "product_description"))

    multi_summary.write_txn_bq_df(transaction_user_agg_df, project_id, data_set_master, summary_user_table_bq)

    multi_summary.write_txn_bq_df(transaction_country_agg_df, project_id, data_set_master, summary_country_table_bq)

    multi_summary.write_txn_bq_df(transaction_product_agg_df, project_id, data_set_master, summary_product_table_bq)


if __name__ == "__main__":
    flow_process_multiple_summary()
