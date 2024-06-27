{% macro return_config_co_f_applications_declination_data_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_declination_data_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-04-14 16:07 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_declination_data_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "applicationdeclined": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "declination_reason",
                "declination_comments",
                "declination_comments_redacted",
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
            "declination_comments",
            "declination_comments_redacted",
            "declination_reason",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}