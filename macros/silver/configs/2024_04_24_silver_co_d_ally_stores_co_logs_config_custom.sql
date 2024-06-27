{% macro return_config_co_d_ally_stores_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=d_ally_stores_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-04-24 11:27 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "d_ally_stores_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "allystorecreated": {
            "direct_attributes": [
                "event_id",
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
            "event_id",
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