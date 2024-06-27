{% macro return_config_br_f_kyc_bureau_personal_info_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_kyc_bureau_personal_info_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-12-09 11:00 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_kyc_bureau_personal_info_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "prospectbureaupersonalinfoobtained": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "personId_birth_city",
                "personId_birth_date",
                "personId_birth_state",
                "personId_education",
                "personId_firstName",
                "personId_fullName",
                "personId_gender",
                "personId_idUpdateDate",
                "personId_lastName",
                "personId_maritalStatus",
                "personId_mother_name_full",
                "personId_number",
                "personId_numberDependents",
                "personId_partnerIdNumber",
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
            "metadata_context_traceId",
            "ocurred_on",
            "personId_birth_city",
            "personId_birth_date",
            "personId_birth_state",
            "personId_education",
            "personId_firstName",
            "personId_fullName",
            "personId_gender",
            "personId_idUpdateDate",
            "personId_lastName",
            "personId_maritalStatus",
            "personId_mother_name_full",
            "personId_number",
            "personId_numberDependents",
            "personId_partnerIdNumber",
            "personId_status",
            "personId_type"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}