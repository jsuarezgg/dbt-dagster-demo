{% macro return_config_co_f_applications_order_details_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_order_details_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-01-26 00:11 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_order_details_co",
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