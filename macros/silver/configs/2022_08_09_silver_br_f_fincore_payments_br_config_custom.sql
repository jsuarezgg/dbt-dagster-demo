{% macro return_config_br_f_fincore_payments_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_fincore_payments_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-08-09 10:34 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_fincore_payments_br",
        "files_db_table_pks": [
            "payment_id"
        ]
    },
    "events": {
        "ClientPaymentAnnulledV2": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
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
