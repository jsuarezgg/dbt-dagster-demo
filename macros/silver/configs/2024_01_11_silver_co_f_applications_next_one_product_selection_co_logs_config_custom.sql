{% macro return_config_co_f_applications_next_one_product_selection_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_next_one_product_selection_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-01-11 08:51 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_next_one_product_selection_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "applicationclosedbynewproductselection": {
            "reference_order_id": 1,
            "direct_attributes": [
                "application_id",
                "client_id",
                "event_id",
                "ocurred_on",
                "ally_slug",
                "product_selection_next_application_id"
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
            "product_selection_next_application_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}