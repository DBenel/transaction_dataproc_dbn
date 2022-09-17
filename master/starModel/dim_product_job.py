#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, round
from master.util.commons import Commons
from master.util.constants import Constants
from master.field.fieldColumn import Fields

class DimProduct:

    def __init__(self):
        self.common = Commons()
        self.constant = Constants()
        self.bank = Fields().BANK
        self.card = Fields().CARD
        self.dim_product = Fields().DIM_PRODUCT

    def read_product_table_df(self, spark, project_id, data_set_master, product_table_bq):
        product_table_path = f"{project_id}.{data_set_master}.{product_table_bq}"
        new_df = self.common.read_bq_table_df(spark, product_table_path)

        return new_df

    def write_product_table_bq_df(self, product_output_df, project_id, data_set_master, product_table_bq):
        product_table_path = f"{project_id}.{data_set_master}.{product_table_bq}"
        self.common.write_bq_df(product_output_df, product_table_path)

    def select_product_card_df(self, card_df):
        product_card_df = card_df.select(
            col(self.card["CARD_ACCOUNT_ID"]).alias(self.dim_product["ACCOUNT_ID"]),
            col(self.dim_product["CARD_TYPE"]),
            col(self.card["CARD_DESCRIPTION"]).alias(self.dim_product["PRODUCT_DESCRIPTION"]),
            col(self.card["CARD_STATUS_ID"]).alias(self.dim_product["PRODUCT_STATUS_ID"])
        )
        return product_card_df

    def select_product_bank_df(self, bank_df):
        product_bank_df = bank_df.select(
            col(self.bank["CARD_ACCOUNT_ID"]).alias(self.dim_product["ACCOUNT_ID"]),
            lit(None).cast("string").alias(self.dim_product["CARD_TYPE"]),
            col(self.bank["BANK_DESCRIPTION"]).alias(self.dim_product["PRODUCT_DESCRIPTION"]),
            col(self.bank["BANK_STATUS_ID"]).alias(self.dim_product["PRODUCT_STATUS_ID"])
        )
        return product_bank_df

def flow_process_dim_product():
    bucket_input = sys.argv[1]
    project_id = sys.argv[2]
    data_set_master = sys.argv[3]
    data_set_star = sys.argv[4]
    bucket_gcs = f"{bucket_input}_tmp"
    card_account_table_bq = Constants().CARD_ACCOUNT_TABLE_BQ
    bank_account_table_bq = Constants().BANK_ACCOUNT_TABLE_BQ
    dim_product_table_bq = Constants().DIM_PRODUCT_TABLE_BQ
    dim_product = DimProduct()

    spark = SparkSession.builder.master("yarn").appName("Dim_Product_Table").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_gcs)

    card_account_df = dim_product.read_product_table_df(spark, project_id, data_set_master, card_account_table_bq)

    bank_account_df = dim_product.read_product_table_df(spark, project_id, data_set_master, bank_account_table_bq)

    product_card_df = dim_product.select_product_card_df(card_account_df)
    product_bank_df = dim_product.select_product_bank_df(bank_account_df)

    dim_product_output_df = product_card_df.union(product_bank_df)

    dim_product.write_product_table_bq_df(dim_product_output_df, project_id, data_set_star, dim_product_table_bq)


if __name__ == "__main__":
    flow_process_dim_product()
