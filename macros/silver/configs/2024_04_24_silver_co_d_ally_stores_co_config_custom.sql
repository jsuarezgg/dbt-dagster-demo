{% macro return_config_co_d_ally_stores_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=d_ally_stores_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-04-24 11:24 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "d_ally_stores_co",
        "files_db_table_pks": [
            "store_slug"
        ]
    },
    "events": {
        "allystorecreated": {
            "direct_attributes": [
                "ally_slug",
                "ally_name",
                "store_slug",
                "store_name",
                "allystorecreated_co_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_name",
            "ally_slug",
            "allystorecreated_co_at",
            "ocurred_on",
            "store_name",
            "store_slug"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}