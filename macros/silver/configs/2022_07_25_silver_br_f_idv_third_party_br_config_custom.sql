{% macro return_config_br_f_idv_third_party_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_idv_third_party_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-07-28 15:30 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_idv_third_party_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "idvthirdpartyapproved": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "idvthirdpartyapproved_br_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartycollected": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "idvthirdpartycollected_br_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyrequested": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "idvthirdpartyrequested_br_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartystarted": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_provider",
                "idvthirdpartystarted_br_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartymanualverificationrequired": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "idvthirdpartymanualverificationrequired_br_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyphotorejected": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "idvthirdpartyphotorejected_br_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyrejected": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "idvthirdpartyrejected_br_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyskipped": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "idv_tp_custom_last_status",
                "idv_tp_unico_error_code",
                "idv_tp_unico_process_id",
                "idv_tp_unico_score",
                "idv_tp_unico_status",
                "idv_tp_provider",
                "idvthirdpartyskipped_br_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "idv_tp_custom_last_status",
            "idv_tp_provider",
            "idv_tp_unico_error_code",
            "idv_tp_unico_process_id",
            "idv_tp_unico_score",
            "idv_tp_unico_status",
            "idvthirdpartyapproved_br_at",
            "idvthirdpartycollected_br_at",
            "idvthirdpartymanualverificationrequired_br_at",
            "idvthirdpartyphotorejected_br_at",
            "idvthirdpartyrejected_br_at",
            "idvthirdpartyrequested_br_at",
            "idvthirdpartyskipped_br_at",
            "idvthirdpartystarted_br_at",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}