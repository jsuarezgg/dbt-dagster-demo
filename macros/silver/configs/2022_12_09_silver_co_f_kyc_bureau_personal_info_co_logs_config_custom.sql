{% macro return_config_co_f_kyc_bureau_personal_info_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_kyc_bureau_personal_info_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-12-09 11:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_kyc_bureau_personal_info_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "prospectbureaupersonalinfoobtained": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "personId_ageRange",
                "personId_expeditionCity",
                "personId_expeditionDate",
                "personId_firstName",
                "personId_fullName",
                "personId_lastName",
                "personId_middleName",
                "personId_number",
                "personId_secondLastName",
                "personId_status",
                "personId_type",
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
            "event_id",
            "metadata_context_traceId",
            "ocurred_on",
            "personId_ageRange",
            "personId_expeditionCity",
            "personId_expeditionDate",
            "personId_firstName",
            "personId_fullName",
            "personId_lastName",
            "personId_middleName",
            "personId_number",
            "personId_secondLastName",
            "personId_status",
            "personId_type"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}