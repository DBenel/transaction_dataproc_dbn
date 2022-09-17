#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col, when, substring, round
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class TransactionCurrency:

    def __init__(self):
        self.common = Commons()
        self.transaction = Fields().TRANSACTION
        self.exchange_rate = Fields().EXCHANGE_RATE
        self.txn_xr = Fields().TRANSACTION_CURRENCY

    def read_txn_table_df(self, spark, project_id, data_set_master, bank_table_bq):
        table_path = f"{project_id}.{data_set_master}.{bank_table_bq}"
        new_df = self.common.read_bq_table_df(spark, table_path)

        return new_df

    def write_txn_bq_df(self, bank_output_df, project_id, data_set_master, bank_table_bq):
        user_table_path = f"{project_id}.{data_set_master}.{bank_table_bq}"
        self.common.write_bq_df(bank_output_df, user_table_path)

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
            ((col(self.exchange_rate["RATE_AMOUNT"]).isNull()) &
             (col(self.transaction["CURRENCY_TYPE"]) == lit("EUR"))),
            round(col(self.transaction["TRANSACTION_AMOUNT"]), 2)
        ).otherwise(
            round(col(self.transaction["TRANSACTION_AMOUNT"]) * col(self.exchange_rate["RATE_AMOUNT"]), 2)
        ).alias(self.txn_xr["LOCAL_CURRENCY_AMOUNT"])

        date_id_column = when(col(self.exchange_rate["DATE_ID"]).isNotNull(),
                                col(self.exchange_rate["DATE_ID"]))\
            .otherwise(substring(col(self.transaction["TRANSACTION_TIMESTAMP"]), 0, 10))\
            .cast("date").alias(self.txn_xr["DATE_ID"])
        month_id_column = concat(
            substring(col(self.transaction["TRANSACTION_TIMESTAMP"]), 0, 4),
            substring(col(self.transaction["TRANSACTION_TIMESTAMP"]), 6, 2)
        ).alias(self.txn_xr["MONTH_ID"])

        new_t_df = transaction_df.select(
            col(self.txn_xr["TRANSACTION_ID"]),
            col(self.txn_xr["ACCOUNT_ID"]),
            col(self.txn_xr["TRANSACTION_TYPE"]),
            col(self.txn_xr["CURRENCY_TYPE"]),
            col(self.txn_xr["RATE_AMOUNT"]),
            col(self.transaction["transaction_amount"]).alias(self.txn_xr["FOREIGN_CURRENCY_AMOUNT"]),
            local_currency_amount_column,
            date_id_column,
            month_id_column,
            substring(col(self.transaction["TRANSACTION_TIMESTAMP"]), 12, 5).alias(self.txn_xr["HOUR_ID"]),
            col(self.txn_xr["TRANSACTION_TIMESTAMP"])
        )

        return new_t_df

def flow_process_transaction_currency():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    exchange_rate_table_bq = Constants().EXCHANGE_RATE_TABLE_BQ
    transaction_table_bq = Constants().TRANSACTION_TABLE_BQ
    txn_xr_table_bq = Constants().TXN_CURRENCY_TABLE_BQ
    txn_xr = TransactionCurrency()

    spark = SparkSession.builder.master("yarn").appName("Transaction_Currency_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    transaction_df = txn_xr.read_txn_table_df(spark, project_id, data_set_master, transaction_table_bq)

    exchange_rate_df = txn_xr.read_txn_table_df(spark, project_id, data_set_master, exchange_rate_table_bq)

    transaction_currency_df = transaction_df\
        .transform(lambda df:  txn_xr.transaction_rate_join(df, exchange_rate_df))\
        .transform(lambda df:  txn_xr.select_transaction_currency(df))

    txn_xr.write_txn_bq_df(transaction_currency_df, project_id, data_set_master, txn_xr_table_bq)


if __name__ == "__main__":
    flow_process_transaction_currency()
