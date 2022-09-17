#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class ExtractExchangeRate:

    def __init__(self):
        self.common = Commons()
        self.exchange_rate = Fields().EXCHANGE_RATE

    def read_exchange_rate_df(self, spark, bucket_input, exchange_rate_file_csv):
        input_exchange_rate_df = self.common.read_raw_csv_cs(spark, bucket_input, exchange_rate_file_csv)
        exchange_rate_df = input_exchange_rate_df \
            .select(
                col("DATE_ID").alias(self.exchange_rate["DATE_ID"]),
                col("CURRENCY").alias(self.exchange_rate["CURRENCY_TYPE"]),
                col("RATE_AMOUNT").alias(self.exchange_rate["RATE_AMOUNT"])
            ).transform(lambda df: self.common.multiple_cast_columns(df, [self.exchange_rate["RATE_AMOUNT"]], "double"))
        return exchange_rate_df

    def write_exchange_rate_bq_df(self, exchange_rate_output_df, project_id, data_set_master, exchange_rate_table_bq):
        exchange_rate_table_path = f"{project_id}.{data_set_master}.{exchange_rate_table_bq}"
        self.common.write_bq_df(exchange_rate_output_df, exchange_rate_table_path)

def flow_process_elt_exchange_rate():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    exchange_rate_file_csv = "EXCHANGE_RATE"
    exchange_rate_table_bq = Constants().EXCHANGE_RATE_TABLE_BQ
    extract_exchange_rate = ExtractExchangeRate()

    spark = SparkSession.builder.master("yarn").appName("Exchange_Rate_Table").getOrCreate()

    transaction_df = extract_exchange_rate.read_exchange_rate_df(spark, bucket_input, exchange_rate_file_csv)

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    extract_exchange_rate.write_exchange_rate_bq_df(transaction_df, project_id, data_set_master, exchange_rate_table_bq)

if __name__ == "__main__":
    flow_process_elt_exchange_rate()
