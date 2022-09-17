#!/usr/bin/env python

import sys
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit, round, when
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class DimUser:

    def __init__(self):
        self.common = Commons()
        self.constant = Constants()
        self.user = Fields().USER
        self.dim_user = Fields().DIM_USER

    def read_user_table_df(self, spark, project_id, data_set_master, user_table_bq):
        user_table_path = f"{project_id}.{data_set_master}.{user_table_bq}"
        new_df = self.common.read_bq_table_df(spark, user_table_path)

        return new_df

    def write_user_table_bq_df(self, user_output_df, project_id, data_set_master, user_table_bq):
        user_table_path = f"{project_id}.{data_set_master}.{user_table_bq}"
        self.common.write_bq_df(user_output_df, user_table_path)

    def user_state_join(self, user_df, state_df):
        return user_df.join(state_df, user_df.user_state_id == state_df.user_state_id, "left")\
            .drop(user_df.user_state_id).drop(state_df.user_state_id)

    def select_user_state_df(self, user_state_df):
        countries = self.constant.COUNTRIES
        mapping_expr = create_map([lit(x) for x in chain(*countries.items())])
        country_name = mapping_expr.getItem(col(self.user["COUNTRY_ID"]))\
            .alias(self.dim_user["COUNTRY_NAME"])

        dim_user_df = user_state_df.select(
            col(self.dim_user["USER_ID"]),
            col(self.dim_user["USER_NAME"]),
            country_name,
            col(self.dim_user["USER_STATE_TYPE"]),
        )
        return dim_user_df

def flow_process_dim_user():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    data_set_star = sys.argv[4]
    bucket_gcs = f"{bucket_input}_tmp"
    user_table_bq = Constants().USER_TABLE_BQ
    user_state_table_bq = Constants().USER_STATE_TABLE_BQ
    dim_user_table_bq = Constants().DIM_USER_TABLE_BQ
    dim_user = DimUser()

    spark = SparkSession.builder.master("yarn").appName("Dim_User_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    user_df = dim_user.read_user_table_df(spark, project_id, data_set_master, user_table_bq)

    user_state_df = dim_user.read_user_table_df(spark, project_id, data_set_master, user_state_table_bq)

    dim_user_df = user_df \
        .transform(lambda df:  dim_user.user_state_join(df, user_state_df)) \
        .transform(lambda df:  dim_user.select_user_state_df(df))

    dim_user.write_user_table_bq_df(dim_user_df, project_id, data_set_star, dim_user_table_bq)


if __name__ == "__main__":
    flow_process_dim_user()
