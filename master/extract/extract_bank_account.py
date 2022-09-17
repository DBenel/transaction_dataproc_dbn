#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class ExtracBank:

    def __init__(self):
        self.common = Commons()
        self.bank = Fields().BANK

    def read_bank_df(self, spark, bucket_input, bank_file_csv):
        input_bank_df = self.common.read_raw_csv_cs(spark, bucket_input, bank_file_csv)
        bank_df = input_bank_df \
            .select(
                col("BANK_ACCOUNT_ID").alias(self.bank["BANK_ACCOUNT_ID"]),
                col("USER_ID").alias(self.bank["USER_ID"]),
                col("FRIENDLY_NAME").alias(self.bank["BANK_DESCRIPTION"]),
                col("IS_ACTIVE").alias(self.bank["BANK_STATUS_ID"]),
                from_unixtime(col("CREATION_TIMESTAMP")).alias(self.bank["BANK_TIMESTAMP"])
            )
        return bank_df

    def write_bank_bq_df(self, bank_output_df, project_id, data_set_master, bank_table_bq):
        user_table_path = f"{project_id}.{data_set_master}.{bank_table_bq}"
        self.common.write_bq_df(bank_output_df, user_table_path)

def flow_process_elt_bank_account():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    bank_file_csv = "BANK_ACCOUNT"
    bank_table_bq = Constants().BANK_ACCOUNT_TABLE_BQ
    extract_bank = ExtracBank()

    spark = SparkSession.builder.master("yarn").appName("Bank_Account_Table").getOrCreate()

    bank_df = extract_bank.read_bank_df(spark, bucket_input, bank_file_csv)

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    extract_bank.write_bank_bq_df(bank_df, project_id, data_set_master, bank_table_bq)


if __name__ == "__main__":
    flow_process_elt_bank_account()
