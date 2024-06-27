{% macro return_config_br_f_kyc_bureau_personal_info_person_id_contingencies_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_kyc_bureau_personal_info_person_id_contingencies_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-12-09 11:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_kyc_bureau_personal_info_person_id_contingencies_br_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "prospectbureaupersonalinfoobtained_unnested_by_person_id_contingency": {
            "direct_attributes": [
                "surrogate_key",
                "item_pseudo_idx",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "bureau_person_id_contingency_idNumber",
                "bureau_person_id_contingency_idType",
                "bureau_person_id_contingency_idTypeDescription",
                "bureau_person_id_contingency_reportDate",
                "bureau_person_id_contingency_reportReason",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "array_parent_path",
            "bureau_person_id_contingency_idNumber",
            "bureau_person_id_contingency_idType",
            "bureau_person_id_contingency_idTypeDescription",
            "bureau_person_id_contingency_reportDate",
            "bureau_person_id_contingency_reportReason",
            "client_id",
            "event_id",
            "item_pseudo_idx",
            "ocurred_on",
            "surrogate_key"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}