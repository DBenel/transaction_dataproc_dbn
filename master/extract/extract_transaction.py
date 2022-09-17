#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class ExtractTransaction:

    def __init__(self):
        self.common = Commons()
        self.transaction = Fields().TRANSACTION

    def read_transaction_df(self, spark, bucket_input, transaction_file_csv):
        input_transaction_df = self.common.read_raw_csv_cs(spark, bucket_input, transaction_file_csv)
        transaction_df = input_transaction_df \
            .select(
                col("transaction_id").alias(self.transaction["TRANSACTION_ID"]),
                col("ACCOUNT_ID").alias(self.transaction["ACCOUNT_ID"]),
                col("TRANSACTION_TYPE").alias(self.transaction["TRANSACTION_TYPE"]),
                col("CURRENCY").alias(self.transaction["CURRENCY_TYPE"]),
                col("AMOUNT").alias(self.transaction["TRANSACTION_AMOUNT"]),
                from_unixtime(col("CREATION_TIMESTAMP")).alias(self.transaction["TRANSACTION_TIMESTAMP"])
            ).transform(lambda df: self.common.multiple_cast_columns(df, [self.transaction["TRANSACTION_AMOUNT"]], "double"))
        return transaction_df

    def write_transaction_bq_df(self, transaction_output_df, project_id, data_set_master, transaction_table_bq):
        transaction_table_path = f"{project_id}.{data_set_master}.{transaction_table_bq}"
        self.common.write_bq_df(transaction_output_df, transaction_table_path)

def flow_process_elt_transaction():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    transaction_file_csv = "TRANSACTION"
    transaction_table_bq = Constants.TRANSACTION_TABLE_BQ
    extract_transaction = ExtractTransaction()

    spark = SparkSession.builder.master("yarn").appName("Transaction_Table").getOrCreate()

    transaction_df = extract_transaction.read_transaction_df(spark, bucket_input, transaction_file_csv)

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    extract_transaction.write_transaction_bq_df(transaction_df, project_id, data_set_master, transaction_table_bq)


if __name__ == "__main__":
    flow_process_elt_transaction()
