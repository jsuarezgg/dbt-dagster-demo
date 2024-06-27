{% macro return_config_br_f_kyc_bureau_contact_info_cellphones_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_kyc_bureau_contact_info_cellphones_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-29 20:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_kyc_bureau_contact_info_cellphones_br_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "prospectbureaucontactinfoobtained_unnested_by_cellphone": {
            "direct_attributes": [
                "surrogate_key",
                "item_pseudo_idx",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "bureau_cellphone_number",
                "bureau_cellphone_order",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "array_parent_path",
            "bureau_cellphone_number",
            "bureau_cellphone_order",
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