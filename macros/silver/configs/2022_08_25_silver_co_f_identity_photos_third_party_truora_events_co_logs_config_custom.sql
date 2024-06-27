{% macro return_config_co_f_identity_photos_third_party_truora_events_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_identity_photos_third_party_truora_events_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-08-25 10:30 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_identity_photos_third_party_truora_events_co_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "identityphotosthirdpartyapproved_unnested_by_truora_event": {
            "direct_attributes": [
                "surrogate_key",
                "truora_event_validation_id",
                "application_id",
                "client_id",
                "truora_event_type",
                "truora_event_validation_status",
                "truora_event_confidence_score",
                "truora_event_declined_reason",
                "truora_event_failure_status",
                "truora_event_threshold",
                "truora_event_timestamp",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartydiscarded_unnested_by_truora_event": {
            "direct_attributes": [
                "surrogate_key",
                "truora_event_validation_id",
                "application_id",
                "client_id",
                "truora_event_type",
                "truora_event_validation_status",
                "truora_event_confidence_score",
                "truora_event_declined_reason",
                "truora_event_failure_status",
                "truora_event_threshold",
                "truora_event_timestamp",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartydiscardedbyrisk_unnested_by_truora_event": {
            "direct_attributes": [
                "surrogate_key",
                "truora_event_validation_id",
                "application_id",
                "client_id",
                "truora_event_type",
                "truora_event_validation_status",
                "truora_event_confidence_score",
                "truora_event_declined_reason",
                "truora_event_failure_status",
                "truora_event_threshold",
                "truora_event_timestamp",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartymanualverificationrequired_unnested_by_truora_event": {
            "direct_attributes": [
                "surrogate_key",
                "truora_event_validation_id",
                "application_id",
                "client_id",
                "truora_event_type",
                "truora_event_validation_status",
                "truora_event_confidence_score",
                "truora_event_declined_reason",
                "truora_event_failure_status",
                "truora_event_threshold",
                "truora_event_timestamp",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyrejected_unnested_by_truora_event": {
            "direct_attributes": [
                "surrogate_key",
                "truora_event_validation_id",
                "application_id",
                "client_id",
                "truora_event_type",
                "truora_event_validation_status",
                "truora_event_confidence_score",
                "truora_event_declined_reason",
                "truora_event_failure_status",
                "truora_event_threshold",
                "truora_event_timestamp",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyrequested_unnested_by_truora_event": {
            "direct_attributes": [
                "surrogate_key",
                "truora_event_validation_id",
                "application_id",
                "client_id",
                "truora_event_type",
                "truora_event_validation_status",
                "truora_event_confidence_score",
                "truora_event_declined_reason",
                "truora_event_failure_status",
                "truora_event_threshold",
                "truora_event_timestamp",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "ocurred_on",
            "surrogate_key",
            "truora_event_confidence_score",
            "truora_event_declined_reason",
            "truora_event_failure_status",
            "truora_event_threshold",
            "truora_event_timestamp",
            "truora_event_type",
            "truora_event_validation_id",
            "truora_event_validation_status"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}