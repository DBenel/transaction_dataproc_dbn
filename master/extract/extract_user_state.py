#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class ExtractUserState:

    def __init__(self):
        self.common = Commons()
        self.user_state = Fields().USER_STATE

    def read_user_state_df(self, spark, bucket_input, user_state_file_csv):
        input_user_state_df = self.common.read_raw_csv_cs(spark, bucket_input, user_state_file_csv)
        user_state_df = input_user_state_df \
            .select(
                col("USER_STATE_ID").alias(self.user_state["USER_STATE_ID"]),
                col("USER_STATE").alias(self.user_state["USER_STATE_TYPE"])
            )
        return user_state_df

    def write_user_state_bq_df(self, user_state_output_df, project_id, data_set_master, user_state_table_bq):
        user_state_table_path = f"{project_id}.{data_set_master}.{user_state_table_bq}"
        self.common.write_bq_df(user_state_output_df, user_state_table_path)

def flow_process_elt_user_state():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    user_state_file_csv = "USER_STATE"
    user_state_table_bq = Constants().USER_STATE_TABLE_BQ
    extract_user_state = ExtractUserState()

    spark = SparkSession.builder.master("yarn").appName("User_Table").getOrCreate()

    user_state_df = extract_user_state.read_user_state_df(spark, bucket_input, user_state_file_csv)

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    extract_user_state.write_user_state_bq_df(user_state_df, project_id, data_set_master, user_state_table_bq)

if __name__ == "__main__":
    flow_process_elt_user_state()
