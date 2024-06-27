{% macro return_config_co_d_prospect_personal_data_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=d_prospect_personal_data_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-05-02 12:59 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "d_prospect_personal_data_co",
        "files_db_table_pks": [
            "client_id"
        ]
    },
    "events": {
        "ProspectBureauPersonalInfoObtained": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "client_id",
                "id_type",
                "id_number",
                "last_name",
                "full_name",
                "document_expedition_city",
                "document_expedition_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PrivacyPolicyAccepted": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "client_id",
                "id_type",
                "id_number",
                "last_name",
                "document_expedition_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PrivacyPolicyAcceptedCO": {
            "stage": "privacy_policy_co",
            "direct_attributes": [
                "client_id",
                "id_type",
                "id_number",
                "last_name",
                "full_name",
                "document_expedition_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PrivacyPolicyAcceptedSantanderCO": {
            "stage": "privacy_policy_santander_co",
            "direct_attributes": [
                "client_id",
                "id_type",
                "id_number",
                "last_name",
                "full_name",
                "document_expedition_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "BasicIdentityValidatedCO": {
            "stage": "basic_identity_co",
            "direct_attributes": [
                "client_id",
                "document_expedition_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PersonalInformationUpdatedCO": {
            "stage": "personal_information_co",
            "direct_attributes": [
                "client_id",
                "id_type",
                "id_number",
                "full_name",
                "birthday",
                "gender",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "birthday",
            "client_id",
            "document_expedition_city",
            "document_expedition_date",
            "full_name",
            "gender",
            "id_number",
            "id_type",
            "last_name",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}