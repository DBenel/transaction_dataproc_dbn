
from pyspark.sql.functions import col, count, lit, round, sum, when
class Fields:
    CREATION_TIMESTAMP = "CREATION_TIMESTAMP"
    USER = {
        "USER_ID": "user_id",
        "USER_NAME": "user_name",
        "COUNTRY_ID": "country_id",
        "USER_STATE_ID": "user_state_id",
        "USER_TIMESTAMP": "user_timestamp"
    }

    USER_STATE = {
        "USER_STATE_ID": "user_state_id",
        "USER_STATE_TYPE": "user_state_type"
    }

    BANK = {
        "BANK_ACCOUNT_ID": "bank_account_id",
        "USER_ID": "user_id",
        "BANK_DESCRIPTION": "bank_description",
        "BANK_STATUS_ID": "bank_status_id",
        "BANK_TIMESTAMP": "bank_timestamp"
    }

    CARD = {
        "CARD_ACCOUNT_ID": "card_account_id",
        "USER_ID": "user_id",
        "CARD_TYPE": "card_type",
        "CARD_DESCRIPTION": "card_description",
        "CARD_STATUS_ID": "card_status_id",
        "CARD_TIMESTAMP": "card_timestamp"
    }

    TRANSACTION = {
        "TRANSACTION_ID" : "transaction_id",
        "ACCOUNT_ID": "account_id",
        "TRANSACTION_TYPE": "transaction_type",
        "CURRENCY_TYPE": "currency_type",
        "TRANSACTION_AMOUNT": "transaction_amount",
        "TRANSACTION_TIMESTAMP": "transaction_timestamp"
    }

    EXCHANGE_RATE = {
        "DATE_ID": "date_id",
        "CURRENCY_TYPE": "currency_type",
        "RATE_AMOUNT": "rate_amount"
    }

    TRANSACTION_CURRENCY = {
        "TRANSACTION_ID": "transaction_id",
        "ACCOUNT_ID": "account_id",
        "TRANSACTION_TYPE": "transaction_type",
        "CURRENCY_TYPE": "currency_type",
        "RATE_AMOUNT": "rate_amount",
        "FOREIGN_CURRENCY_AMOUNT": "foreign_currency_amount",
        "LOCAL_CURRENCY_AMOUNT": "local_currency_amount",
        "DATE_ID": "date_id",
        "MONTH_ID": "month_id",
        "HOUR_ID": "hour_id",
        "TRANSACTION_TIMESTAMP": "transaction_timestamp"
    }

    TRANSACTION_MASTER = {
        "USER_ID": "user_id",
        "TRANSACTION_ID": "transaction_id",
        "ACCOUNT_ID": "account_id",
        "DATE_ID": "date_id",
        "USER_NAME": "user_name",
        "COUNTRY_NAME": "country_name",
        "USER_STATE_TYPE": "user_state_type",
        "CARD_TYPE": "card_type",
        "PRODUCT_DESCRIPTION": "product_description",
        "PRODUCT_STATUS_ID": "product_status_id",
        "TRANSACTION_TYPE": "transaction_type",
        "CURRENCY_TYPE": "currency_type",
        "RATE_AMOUNT": "rate_amount",
        "FOREIGN_CURRENCY_AMOUNT": "foreign_currency_amount",
        "LOCAL_CURRENCY_AMOUNT": "local_currency_amount",
        "MONTH_ID": "month_id",
        "HOUR_ID": "hour_id",
        "TRANSACTION_TIMESTAMP": "transaction_timestamp"
    }

    DIM_USER = {
        "USER_ID": "user_id",
        "USER_NAME": "user_name",
        "COUNTRY_NAME": "country_NAME",
        "USER_STATE_TYPE": "user_state_TYPE"
    }

    DIM_PRODUCT = {
        "ACCOUNT_ID": "account_id",
        "USER_ID": "user_id",
        "CARD_TYPE": "card_type",
        "PRODUCT_DESCRIPTION": "product_description",
        "PRODUCT_STATUS_ID": "product_status_id"
    }

    FACT_TRANSACTION = {
        "TRANSACTION_ID": "transaction_id",
        "ACCOUNT_ID": "account_id",
        "USER_ID": "user_id",
        "DATE_ID": "date_id",
        "TRANSACTION_TYPE": "transaction_type",
        "CURRENCY_TYPE": "currency_type",
        "RATE_AMOUNT": "rate_amount",
        "FOREIGN_CURRENCY_AMOUNT": "foreign_currency_amount",
        "LOCAL_CURRENCY_AMOUNT": "local_currency_amount",
        "TRANSACTION_TIMESTAMP": "transaction_timestamp"
    }
