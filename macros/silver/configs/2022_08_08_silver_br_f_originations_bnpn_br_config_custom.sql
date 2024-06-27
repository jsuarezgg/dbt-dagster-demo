{% macro return_config_br_f_originations_bnpn_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_originations_bnpn_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-23 18:05 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_originations_bnpn_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "PixPaymentReceivedBR": {
            "stage": "bn_pn_payments_br",
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "origination_date",
                "requested_amount",
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
            "ocurred_on",
            "origination_date",
            "requested_amount"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}