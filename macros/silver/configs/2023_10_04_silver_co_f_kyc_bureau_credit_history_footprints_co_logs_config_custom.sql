{% macro return_config_co_f_kyc_bureau_credit_history_footprints_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_kyc_bureau_credit_history_footprints_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-10-04 16:23 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_kyc_bureau_credit_history_footprints_co_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "prospectbureaucredithistoryobtained_unnested_by_footprints": {
            "direct_attributes": [
                "surrogate_key",
                "item_pseudo_idx",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "bureau_footprint_branch",
                "bureau_footprint_city",
                "bureau_footprint_creditPullReason",
                "bureau_footprint_date",
                "bureau_footprint_entityName",
                "bureau_footprint_entityType",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "array_parent_path",
            "bureau_footprint_branch",
            "bureau_footprint_city",
            "bureau_footprint_creditPullReason",
            "bureau_footprint_date",
            "bureau_footprint_entityName",
            "bureau_footprint_entityType",
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