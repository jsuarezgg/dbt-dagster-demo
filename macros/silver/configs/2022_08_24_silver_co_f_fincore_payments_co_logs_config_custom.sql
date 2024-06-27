{% macro return_config_co_f_fincore_payments_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_fincore_payments_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-01-06 12:12 TZ-0600",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_fincore_payments_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "ClientPaymentAnnulledV2": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "payment_id",
                "is_annulled",
                "annulment_reason",
                "ocurred_on",
                "client_id"
            ],
            "custom_attributes": {}
        },
        "ClientPaymentReceivedV3": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "payment_id",
                "client_id",
                "payment_method",
                "paid_amount",
                "payment_date",
                "is_annulled",
                "annulment_reason",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "WompiTransactionCreated": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "payment_id",
                "client_id",
                "wompi_transaction_status",
                "wompi_transaction_status_reason",
                "custom_is_wompi",
                "wompi_transaction_financial_institution_code",
                "custom_nequi_payment_source",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "WompiTransactionUpdated": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "payment_id",
                "client_id",
                "wompi_transaction_status",
                "wompi_transaction_status_reason",
                "wompi_business_agreement_code",
                "wompi_payment_intention_identifier",
                "custom_is_wompi",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "annulment_reason",
            "client_id",
            "custom_is_wompi",
            "event_id",
            "is_annulled",
            "ocurred_on",
            "paid_amount",
            "payment_date",
            "payment_id",
            "payment_method",
            "wompi_business_agreement_code",
            "wompi_payment_intention_identifier",
            "wompi_transaction_status",
            "wompi_transaction_status_reason",
            "wompi_transaction_financial_institution_code",
            "custom_nequi_payment_source"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
