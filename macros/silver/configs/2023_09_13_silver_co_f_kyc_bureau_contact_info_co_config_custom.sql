{% macro return_config_co_f_kyc_bureau_contact_info_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_kyc_bureau_contact_info_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-09-13 16:15 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_kyc_bureau_contact_info_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "prospectbureaucontactinfoobtained": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "metadata_context_traceId",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "metadata_context_traceId",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}