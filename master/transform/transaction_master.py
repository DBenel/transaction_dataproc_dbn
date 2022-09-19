#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, round
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class TransactionMaster:

    def __init__(self):
        self.common = Commons()
        self.user = Fields().USER
        self.user_st = Fields().USER_STATE
        self.bank = Fields().BANK
        self.card = Fields().CARD
        self.txn_xr = Fields().TRANSACTION_CURRENCY
        self.txn_master = Fields().TRANSACTION_MASTER

    def read_txn_table_df(self, spark, project_id, data_set_master, txn_table_bq):
        table_path = f"{project_id}.{data_set_master}.{txn_table_bq}"
        new_df = self.common.read_bq_table_df(spark, table_path)

        return new_df

    def write_txn_table_bq_df(self, txn_output_df, project_id, data_set_master, txn_table_bq):
        txn_table_path = f"{project_id}.{data_set_master}.{txn_table_bq}"
        self.common.write_bq_df(txn_output_df, txn_table_path)


    def user_state_join(self, user_df, state_df):
        return user_df.join(state_df, user_df.user_state_id == state_df.user_state_id, "left")\
            .drop(user_df.user_state_id).drop(state_df.user_state_id)

    def match_status_column(self, name):
        if (name == "card_status_id") or (name == "bank_status_id"):
            return when(col(name) == "0", lit("NOT ACTIVE"))\
                .otherwise(lit("ACTIVE")).alias(self.txn_master["PRODUCT_STATUS_ID"])
        else:
            return col(name)

    def user_product_join(self, user_df, product_df):
        new_df = user_df.join(product_df, user_df.user_id == product_df.user_id, "left") \
            .drop(product_df.user_id)
        return new_df.select(
            *list(map(lambda c: self.match_status_column(c), new_df.columns))
        )

    def user_card_transaction(self, user_card_df,transaction_df):
        new_user_card_df = user_card_df \
            .join(
                transaction_df,
                user_card_df.card_account_id == transaction_df.account_id,
                "left"
            ) \
            .drop(transaction_df.account_id)
        return new_user_card_df

    def user_bank_transaction(self, user_bank_df,transaction_df):
        new_user_bank_df = user_bank_df \
            .join(
                transaction_df,
                user_bank_df.bank_account_id == transaction_df.account_id,
                "left"
            ) \
            .drop(transaction_df.account_id)
        return new_user_bank_df

    def rename_columns(self, name, column_list):
        if name == "account_id":
            if "card_account_id" in column_list:
                return col(self.card["CARD_ACCOUNT_ID"]).alias(self.txn_master["ACCOUNT_ID"])
            elif "bank_account_id" in column_list:
                return col(self.bank["BANK_ACCOUNT_ID"]).alias(self.txn_master["ACCOUNT_ID"])
        elif name == "card_type":
            return lit(None).cast("string").alias(self.txn_master["CARD_TYPE"])
        elif name == "product_description":
            if "card_description" in column_list:
                return col(self.card["CARD_DESCRIPTION"]).alias(self.txn_master["PRODUCT_DESCRIPTION"])
            elif "bank_description" in column_list:
                return col(self.bank["BANK_DESCRIPTION"]).alias(self.txn_master["PRODUCT_DESCRIPTION"])

    def select_txn_product_df(self, txn_product_df):
        column_master = self.txn_master.values()
        col_input = txn_product_df.columns
        new_txn_product_df = txn_product_df.select(
            *list(map(lambda c: col(c) if c in col_input else self.rename_columns(c,col_input), column_master))
        )
        return new_txn_product_df

def flow_process_transaction_master():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    user_table_bq = Constants().USER_TABLE_BQ
    user_state_table_bq = Constants().USER_STATE_TABLE_BQ
    card_table_bq = Constants().CARD_ACCOUNT_TABLE_BQ
    bank_table_bq = Constants().BANK_ACCOUNT_TABLE_BQ
    txn_xr_table_bq = Constants().TXN_CURRENCY_TABLE_BQ
    txn_master_bq = Constants().TXN_MASTER_TABLE_BQ
    txn_master = TransactionMaster()

    spark = SparkSession.builder.master("yarn").appName("Transaction_Master_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    user_df = txn_master.read_txn_table_df(spark, project_id, data_set_master, user_table_bq)

    user_state_df = txn_master.read_txn_table_df(spark, project_id, data_set_master, user_state_table_bq)

    p_card_account_df = txn_master.read_txn_table_df(spark, project_id, data_set_master, card_table_bq)

    p_bank_account_df = txn_master.read_txn_table_df(spark, project_id, data_set_master, bank_table_bq)

    transaction_currency_df = txn_master.read_txn_table_df(spark, project_id, data_set_master, txn_xr_table_bq)

    users_new_df = user_df \
        .transform(lambda df:  txn_master.user_state_join(df, user_state_df))\
        .transform(lambda df: Commons().select_user_state_df(df))

    user_card_df = users_new_df \
        .transform(lambda df: txn_master.user_product_join(df, p_card_account_df))\
        .transform(lambda df: txn_master.user_card_transaction(df, transaction_currency_df))\
        .transform(lambda df: txn_master.select_txn_product_df(df))

    user_bank_df = users_new_df \
        .transform(lambda df: txn_master.user_product_join(df, p_bank_account_df))\
        .transform(lambda df: txn_master.user_bank_transaction(df, transaction_currency_df))\
        .transform(lambda df: txn_master.select_txn_product_df(df))

    master_df = user_card_df.union(user_bank_df)

    txn_master.write_txn_table_bq_df(master_df, project_id, data_set_master, txn_master_bq)


if __name__ == "__main__":
    flow_process_transaction_master()
