{% macro return_config_co_f_transaction_marked_leadgen_attributable_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_transaction_marked_leadgen_attributable_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-12-11 07:45 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_transaction_marked_leadgen_attributable_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "transactionmarkedleadgenattributable": {
            "direct_attributes": [
                "addishop_fee",
                "addishop_config_id",
                "addishop_hours_for_atrribution",
                "addishop_show",
                "ally_shop_slug_associated",
                "ally_slug",
                "application_id",
                "channel",
                "client_id",
                "is_addishop_active",
                "origination_date",
                "ocurred_on"
            ]
        }
    },
    "unique_db_fields": {
        "direct": [
            "addishop_fee",
            "addishop_config_id",
            "addishop_hours_for_atrribution",
            "addishop_show",
            "ally_shop_slug_associated",
            "ally_slug",
            "application_id",
            "channel",
            "client_id",
            "is_addishop_active",
            "ocurred_on",
            "origination_date"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}