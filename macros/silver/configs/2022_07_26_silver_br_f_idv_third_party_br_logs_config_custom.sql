{% macro return_config_br_f_idv_third_party_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_idv_third_party_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-07-28 15:30 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_idv_third_party_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "idvthirdpartyapproved": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartycollected": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyrequested": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartystarted": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_provider",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartymanualverificationrequired": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyphotorejected": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyrejected": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyskipped": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
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
            "idv_tp_custom_last_status",
            "idv_tp_provider",
            "idv_tp_unico_error_code",
            "idv_tp_unico_process_id",
            "idv_tp_unico_score",
            "idv_tp_unico_status",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}