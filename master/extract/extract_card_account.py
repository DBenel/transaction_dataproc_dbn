#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class ExtractCard:

    def __init__(self):
        self.common = Commons()
        self.card = Fields().CARD

    def read_card_df(self, spark, bucket_input, card_file_csv):
        input_card_df = self.common.read_raw_csv_cs(spark, bucket_input, card_file_csv)
        card_df = input_card_df \
            .select(
                col("CARD_ACCOUNT_ID").alias(self.card["CARD_ACCOUNT_ID"]),
                col("USER_ID").alias(self.card["USER_ID"]),
                col("CARD_TYPE").alias(self.card["CARD_TYPE"]),
                col("FRIENDLY_NAME").alias(self.card["CARD_DESCRIPTION"]),
                col("IS_ACTIVE").alias(self.card["CARD_STATUS_ID"]),
                from_unixtime(col("CREATION_TIMESTAMP")).alias(self.card["CARD_TIMESTAMP"])
            )
        return card_df

    def write_card_bq_df(self, card_output_df, project_id, data_set_master, card_table_bq):
        card_table_path = f"{project_id}.{data_set_master}.{card_table_bq}"
        self.common.write_bq_df(card_output_df, card_table_path)

def flow_process_elt_card_account():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    card_file_csv = "CARD_ACCOUNT"
    card_table_bq = Constants().CARD_ACCOUNT_TABLE_BQ
    extract_card = ExtractCard()

    spark = SparkSession.builder.master("yarn").appName('Card_Account_Table').getOrCreate()

    card_df = extract_card.read_card_df(spark, bucket_input, card_file_csv)

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    extract_card.write_card_bq_df(card_df, project_id, data_set_master, card_table_bq)

if __name__ == "__main__":
    flow_process_elt_card_account()
