#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class ExtractUser:

    def __init__(self):
        self.common = Commons()
        self.user = Fields().USER

    def read_user_df(self, spark, bucket_input, user_file_csv):
        input_user_df = self.common.read_raw_csv_cs(spark, bucket_input, user_file_csv)
        user_df = input_user_df \
            .select(
                col("USER_ID").alias(self.user["USER_ID"]),
                col("USERNAME").alias(self.user["USER_NAME"]),
                col("COUNTRY").alias(self.user["COUNTRY_ID"]),
                col("USER_STATE_ID").alias(self.user["USER_STATE_ID"]),
                from_unixtime(col("CREATION_TIMESTAMP")).alias(self.user["USER_TIMESTAMP"])
            )
        return user_df

    def write_user_bq_df(self, user_output_df, project_id, data_set_master, user_table_bq):
        user_table_path = f"{project_id}.{data_set_master}.{user_table_bq}"
        self.common.write_bq_df(user_output_df, user_table_path)

def flow_process_elt_users():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    bucket_gcs = f"{bucket_input}_tmp"
    user_file_csv = "USER"
    user_table_bq = Constants().USER_TABLE_BQ
    extract_user = ExtractUser()

    spark = SparkSession.builder.master("yarn").appName("User_Table").getOrCreate()

    user_df = extract_user.read_user_df(spark, bucket_input, user_file_csv)

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    extract_user.write_user_bq_df(user_df, project_id, data_set_master, user_table_bq)

if __name__ == "__main__":
    flow_process_elt_users()
