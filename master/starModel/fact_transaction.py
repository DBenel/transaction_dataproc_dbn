#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col, when, substring, round
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class FactTransaction:

    def __init__(self):
        self.common = Commons()
        self.constant = Constants()
        self.transaction = Fields().TRANSACTION
        self.exchange_rate = Fields().EXCHANGE_RATE
        self.dim_product = Fields().DIM_PRODUCT
        self.fact_transaction = Fields().FACT_TRANSACTION

    def read_txn_table_df(self, spark, project_id, data_set_master, txn_table_bq):
        txn_table_path = f"{project_id}.{data_set_master}.{txn_table_bq}"
        new_df = self.common.read_bq_table_df(spark, txn_table_path)

        return new_df

    def write_txn_table_bq_df(self, txn_output_df, project_id, data_set_master, txn_table_bq):
        txn_table_path = f"{project_id}.{data_set_master}.{txn_table_bq}"
        self.common.write_bq_df(txn_output_df, txn_table_path)

    def transaction_rate_join(self, transaction_df, exchange_rate_df):
        new_join_df = transaction_df \
            .join(
                exchange_rate_df,
                ((substring(transaction_df.transaction_timestamp, 0, 10) == exchange_rate_df.date_id) &
                 (transaction_df.currency_type == exchange_rate_df.currency_type)),
                "left"
            ).drop(exchange_rate_df.currency_type)

        return new_join_df

    def select_transaction_currency(self, transaction_df):
        local_currency_amount_column = when(
            ((col(self.exchange_rate["RATE_AMOUNT"]).isNull()) & (col(self.transaction["CURRENCY_TYPE"]) == lit("EUR"))),
            col(self.transaction["TRANSACTION_AMOUNT"])
        ).otherwise(
            round(col(self.transaction["TRANSACTION_AMOUNT"]) * col(self.exchange_rate["RATE_AMOUNT"]), 2)
        ).alias(self.fact_transaction["LOCAL_CURRENCY_AMOUNT"])

        date_id_column = when(col(self.exchange_rate["DATE_ID"]).isNotNull(), col(self.exchange_rate["DATE_ID"]))\
            .otherwise(substring(col(self.transaction["transaction_timestamp"]), 0, 10))\
            .cast("date").alias(self.fact_transaction["DATE_ID"])

        new_t_df = transaction_df.select(
            col(self.fact_transaction["TRANSACTION_ID"]),
            col(self.fact_transaction["ACCOUNT_ID"]),
            date_id_column,
            col(self.fact_transaction["CURRENCY_TYPE"]),
            col(self.fact_transaction["TRANSACTION_TYPE"]),
            col(self.fact_transaction["RATE_AMOUNT"]),
            col(self.transaction["TRANSACTION_AMOUNT"]).alias(self.fact_transaction["FOREIGN_CURRENCY_AMOUNT"]),
            local_currency_amount_column,
            col(self.fact_transaction["TRANSACTION_TIMESTAMP"])
        )

        return new_t_df

    def select_user_id(self, product_df):
        return product_df.select(
            col(self.dim_product["ACCOUNT_ID"]),
            col(self.dim_product["USER_ID"]))

    def transaction_user_join(self, transaction_df, user_df):
        transaction_user_df = transaction_df.join(user_df,
            transaction_df.account_id == user_df.account_id, "left")\
            .drop(user_df.account_id)

        return  transaction_user_df \
            .select(
                *list(map(lambda c: col(c), self.fact_transaction.values()))
            )


def flow_process_fact_transaction():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    data_set_star = sys.argv[4]
    bucket_gcs = f"{bucket_input}_tmp"
    transaction_table_bq = Constants().TRANSACTION_TABLE_BQ
    exchange_rate_table_bq = Constants().EXCHANGE_RATE_TABLE_BQ
    dim_product_table_bq = Constants().DIM_PRODUCT_TABLE_BQ
    fact_transaction_table_bq = Constants().FACT_TRANSACTION_TABLE_BQ
    fact_transaction = FactTransaction()

    spark = SparkSession.builder.master("yarn").appName("Fact_Transaction_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    transaction_df = fact_transaction.read_txn_table_df(spark, project_id, data_set_master, transaction_table_bq)

    exchange_rate_df = fact_transaction.read_txn_table_df(spark, project_id, data_set_master, exchange_rate_table_bq)

    dim_product_df = fact_transaction.read_txn_table_df(spark, project_id, data_set_star, dim_product_table_bq)

    user_id_df = fact_transaction.select_user_id(dim_product_df)

    fact_txn_output_df = transaction_df\
        .transform(lambda df:  fact_transaction.transaction_rate_join(df, exchange_rate_df))\
        .transform(lambda df:  fact_transaction.select_transaction_currency(df)) \
        .transform(lambda df:  fact_transaction.transaction_user_join(df, user_id_df))

    fact_transaction.write_txn_table_bq_df(fact_txn_output_df, project_id, data_set_star, fact_transaction_table_bq)


if __name__ == "__main__":
    flow_process_fact_transaction()
