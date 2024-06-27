{% macro return_config_co_f_identity_photos_third_party_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_identity_photos_third_party_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-08-25 10:30 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_identity_photos_third_party_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "identityphotosthirdpartyapproved": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "identityphotosthirdpartyapproved_co_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartydiscarded": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "identityphotosthirdpartydiscarded_co_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartydiscardedbyrisk": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "identityphotosthirdpartydiscardedbyrisk_co_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartymanualverificationrequired": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "identityphotosthirdpartymanualverificationrequired_co_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyrejected": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "identityphotosthirdpartyrejected_co_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyrequested": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "identityphotosthirdpartyrequested_co_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartystarted": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "identity_photos_tp_custom_last_status",
                "idv_provider",
                "idv_third_party_attempts",
                "identityphotosthirdpartystarted_co_at",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "identity_photos_tp_custom_last_status",
            "identityphotosthirdpartyapproved_co_at",
            "identityphotosthirdpartydiscarded_co_at",
            "identityphotosthirdpartydiscardedbyrisk_co_at",
            "identityphotosthirdpartymanualverificationrequired_co_at",
            "identityphotosthirdpartyrejected_co_at",
            "identityphotosthirdpartyrequested_co_at",
            "identityphotosthirdpartystarted_co_at",
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