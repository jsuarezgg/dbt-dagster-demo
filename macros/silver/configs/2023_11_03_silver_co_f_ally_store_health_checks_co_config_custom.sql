{% macro return_config_co_f_ally_store_health_checks_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_ally_store_health_checks_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-11-06 15:21 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_ally_store_health_checks_co",
        "files_db_table_pks": [
            "ally_slug"
        ]
    },
    "events": {
        "storehealthcheckreceived": {
            "direct_attributes": [
                "ally_slug",
                "healthcheck_checkout_methods",
                "healthcheck_checkout_position",
                "healthcheck_component",
                "healthcheck_datetime",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "healthcheck_checkout_methods",
            "healthcheck_checkout_position",
            "healthcheck_component",
            "healthcheck_datetime",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}