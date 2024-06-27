{% macro return_config_co_f_temporary_refinance_loans_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_temporary_refinance_loans_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-10-31 05:44 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_temporary_refinance_loans_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "refinanceloanacceptedco": {
            "direct_attributes": [
                "application_id",
                "loan_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "loan_id",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}