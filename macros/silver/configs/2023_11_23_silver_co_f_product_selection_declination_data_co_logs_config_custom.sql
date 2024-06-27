{% macro return_config_co_f_product_selection_declination_data_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_product_selection_declination_data_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-11-23 15:53 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_product_selection_declination_data_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "productselectiondeclined": {
            "reference_order_id": 1,
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "ally_slug",
                "event_name",
                "client_type",
                "channel",
                "order_id",
                "declination_reason",
                "declination_comments"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "channel",
            "client_id",
            "event_name",
            "client_type",
            "ocurred_on",
            "order_id",
            "declination_reason",
            "declination_comments"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}