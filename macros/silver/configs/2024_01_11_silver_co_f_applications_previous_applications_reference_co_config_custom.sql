{% macro return_config_co_f_applications_previous_applications_reference_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_previous_applications_reference_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-01-11 08:51 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_previous_applications_reference_co",
        "files_db_table_pks": [
            "custom_application_reference_id"
        ]
    },
    "events": {
        "applicationcreated_unnested_by_previous_ids": {
            "reference_order_id": 1,
            "direct_attributes": [
                "application_id",
                "client_id",
                "custom_application_reference_id",
                "event_id",
                "ocurred_on",
                "ally_slug",
                "previous_id"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "client_id",
            "custom_application_reference_id",
            "event_id",
            "ocurred_on",
            "previous_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}