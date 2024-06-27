{% macro return_config_co_f_wompi_transactions_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_wompi_transactions_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-04-17 12:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_wompi_transactions_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "wompitransactioncreatedv2": {
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "client_id",
                "channel",
                "product",
                "transaction_amount",
                "transaction_currency",
                "transaction_origin",
                "transaction_financial_institution_code",
                "transaction_payment_description",
                "transaction_type",
                "transaction_reference",
                "transaction_status",
                "transaction_status_reason"
            ],
            "custom_attributes": {}
        },
        "wompitransactionupdatedv2": {
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "client_id",
                "transaction_amount",
                "transaction_currency",
                "transaction_origin",
                "transaction_business_agreement_code",
                "transaction_payment_intention_identifier",
                "transaction_financial_institution_code",
                "transaction_payment_description",
                "transaction_type",
                "transaction_reference",
                "transaction_status",
                "transaction_status_reason"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "channel",
            "client_id",
            "event_id",
            "ocurred_on",
            "product",
            "transaction_amount",
            "transaction_business_agreement_code",
            "transaction_currency",
            "transaction_financial_institution_code",
            "transaction_origin",
            "transaction_payment_description",
            "transaction_payment_intention_identifier",
            "transaction_reference",
            "transaction_status",
            "transaction_status_reason",
            "transaction_type"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
