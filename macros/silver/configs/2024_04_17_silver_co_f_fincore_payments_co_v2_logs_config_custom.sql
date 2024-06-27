{% macro return_config_co_f_fincore_payments_co_v2_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_fincore_payments_co_v2_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-04-17 12:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_fincore_payments_co_v2_logs",
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
        }
    },
    "unique_db_fields": {
        "direct": [
            "annulment_reason",
            "client_id",
            "event_id",
            "is_annulled",
            "ocurred_on",
            "paid_amount",
            "payment_date",
            "payment_id",
            "payment_method"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
