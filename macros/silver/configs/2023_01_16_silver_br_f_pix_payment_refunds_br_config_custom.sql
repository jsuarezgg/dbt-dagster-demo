{% macro return_config_br_f_pix_payment_refunds_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_pix_payment_refunds_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-01-18 12:01 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_pix_payment_refunds_br",
        "files_db_table_pks": [
            "pix_payment_number"
        ]
    },
    "events": {
        "pixpaymentrefunded": {
            "direct_attributes": [
                "pix_payment_number",
                "ally_slug",
                "client_id",
                "user_id",
                "pix_payment_amount",
                "pix_payment_refund_occurred_on",
                "pix_payment_refund_reason",
                "custom_event_domain",
                "custom_payment_refund_status",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "client_id",
            "custom_event_domain",
            "custom_payment_refund_status",
            "ocurred_on",
            "pix_payment_amount",
            "pix_payment_number",
            "pix_payment_refund_occurred_on",
            "pix_payment_refund_reason",
            "user_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}