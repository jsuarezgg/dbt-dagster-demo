{% macro return_config_br_f_kyc_iovation_v1v2_result_rules_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_kyc_iovation_v1v2_result_rules_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-22 18:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_kyc_iovation_v1v2_result_rules_br_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "iovationobtained_unnested_by_result_rule": {
            "direct_attributes": [
                "surrogate_key",
                "item_pseudo_idx",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "custom_kyc_event_version",
                "iovation_rule_result_reason",
                "iovation_rule_result_score",
                "iovation_rule_result_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "iovationobtained_v2_unnested_by_result_rule": {
            "direct_attributes": [
                "surrogate_key",
                "item_pseudo_idx",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "custom_kyc_event_version",
                "iovation_rule_result_reason",
                "iovation_rule_result_score",
                "iovation_rule_result_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "array_parent_path",
            "client_id",
            "custom_kyc_event_version",
            "event_id",
            "iovation_rule_result_reason",
            "iovation_rule_result_score",
            "iovation_rule_result_type",
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