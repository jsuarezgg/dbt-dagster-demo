{% macro return_config_br_f_applications_order_details_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_applications_order_details_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-01-26 00:11 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_applications_order_details_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "ApplicationCreated": {
            "direct_attributes": [
                "ally_slug",
                "application_id",
                "client_id",
                "ocurred_on",
                "shipping_address",
                "shipping_city"
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
            "shipping_address",
            "shipping_city"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}