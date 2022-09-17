class Constants:
    PATH_CLOUD_STORAGE = "/transaction_dataproc_dbn/resources/data/input/"
    USER_TABLE_BQ = "user"
    USER_STATE_TABLE_BQ = "user_state"
    CARD_ACCOUNT_TABLE_BQ = "card_account"
    BANK_ACCOUNT_TABLE_BQ = "bank_account"
    TRANSACTION_TABLE_BQ = "transaction"
    EXCHANGE_RATE_TABLE_BQ = "exchange_rate"
    TXN_CURRENCY_TABLE_BQ = "transaction_currency"
    TXN_MASTER_TABLE_BQ = "transaction_master"


    SUMMARY_USER_TABLE_BQ = "summary_user"
    SUMMARY_COUNTRY_TABLE_BQ = "summary_country"
    SUMMARY_PRODUCT_TABLE_BQ = "summary_product"

    DIM_USER_TABLE_BQ = "dim_user"
    DIM_PRODUCT_TABLE_BQ = "dim_product"
    FACT_TRANSACTION_TABLE_BQ = "fact_transaction"

    COUNTRIES = {
        "DE": "Germany",
        "ES": "Spain",
        "MT": "Malta",
        "EN": "United Kingdom"
    }
