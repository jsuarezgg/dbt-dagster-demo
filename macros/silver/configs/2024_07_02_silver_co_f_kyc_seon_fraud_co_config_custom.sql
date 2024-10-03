{% macro return_config_co_f_kyc_seon_fraud_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_kyc_seon_fraud_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-07-02 11:57 TZ-0600",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_kyc_seon_fraud_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "SeonFraudObtained": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "ocurred_on",
                "application_id",
                "client_id",
                "seon_state",
                "seon_fraud_score",
                "seon_applied_rules",
                "seon_ip_details_score",
                "seon_email_details_score",
                "seon_email_details_domain_created",
                "seon_email_details_domain_updated",
                "seon_email_details_account_details",
                "seon_email_details_breach_have_i_been_pwned_listed",
                "seon_email_details_breach_number_of_breaches",
                "seon_email_details_breach_first_breach",
                "seon_phone_details_score",
                "seon_phone_details_carrier",
                "seon_phone_details_account_details",
                "seon_aml_details_has_watchlist_match",
                "seon_aml_details_has_sanction_match",
                "seon_aml_details_has_crimelist_match",
                "seon_aml_details_has_pep_match"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "ocurred_on",
            "seon_aml_details_has_crimelist_match",
            "seon_aml_details_has_pep_match",
            "seon_aml_details_has_sanction_match",
            "seon_aml_details_has_watchlist_match",
            "seon_applied_rules",
            "seon_email_details_account_details",
            "seon_email_details_breach_first_breach",
            "seon_email_details_breach_have_i_been_pwned_listed",
            "seon_email_details_breach_number_of_breaches",
            "seon_email_details_domain_created",
            "seon_email_details_domain_updated",
            "seon_email_details_score",
            "seon_fraud_score",
            "seon_ip_details_score",
            "seon_phone_details_account_details",
            "seon_phone_details_carrier",
            "seon_phone_details_score",
            "seon_state"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
