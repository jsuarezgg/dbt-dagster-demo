{% macro return_config_co_f_identity_photos_third_party_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_identity_photos_third_party_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-08-25 10:30 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_identity_photos_third_party_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "identityphotosthirdpartyapproved": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartydiscarded": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartydiscardedbyrisk": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartymanualverificationrequired": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyrejected": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyrequested": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartystarted": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
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
            "identity_photos_tp_custom_last_status",
            "idv_provider",
            "idv_third_party_attempts",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}