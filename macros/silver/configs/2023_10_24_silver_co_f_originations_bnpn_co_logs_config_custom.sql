{% macro return_config_co_f_originations_bnpn_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_originations_bnpn_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-10-31 17:49 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_originations_bnpn_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "BnpnPaymentApprovedCO": {
            "stage": "bnpn_summary_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "pse_amount",
                "payment_amount",
                "ally_slug",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "client_id",
            "event_id",
            "ocurred_on",
            "payment_amount",
            "pse_amount",
            "store_slug",
            "store_user_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}