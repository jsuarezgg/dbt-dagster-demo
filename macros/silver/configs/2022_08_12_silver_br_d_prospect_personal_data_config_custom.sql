{% macro return_config_br_d_prospect_personal_data_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=d_prospect_personal_data_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-08-10 15:22 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "d_prospect_personal_data_br",
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
                "first_name",
                "full_name",
                "birth_date",
                "birth_city",
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
                "first_last_name",
                "birth_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PrivacyPolicyAcceptedBR": {
            "stage": "privacy_policy_br",
            "direct_attributes": [
                "client_id",
                "id_type",
                "id_number",
                "first_name",
                "full_name",
                "birth_date",
                "first_last_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CheckoutLoginAcceptedBR": {
            "stage": "expedited_checkout_login_br",
            "direct_attributes": [
                "client_id",
                "id_type",
                "id_number",
                "first_name",
                "full_name",
                "birth_date",
                "first_last_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "BasicIdentityValidatedBR": {
            "stage": "basic_identity_br",
            "direct_attributes": [
                "client_id",
                "birth_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "birth_city",
            "birth_date",
            "first_last_name",
            "first_name",
            "full_name",
            "id_number",
            "id_type",
            "ocurred_on",
            "client_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
