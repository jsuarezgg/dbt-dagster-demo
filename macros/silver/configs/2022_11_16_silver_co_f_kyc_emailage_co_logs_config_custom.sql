{% macro return_config_co_f_kyc_emailage_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_kyc_emailage_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-16 20:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_kyc_emailage_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "emailageobtained": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "emailAge_advice_id",
                "emailAge_advice_value",
                "emailAge_birthDate",
                "emailAge_company",
                "emailAge_country",
                "emailAge_domain_age",
                "emailAge_domain_creationDays",
                "emailAge_domain_exits",
                "emailAge_domain_name",
                "emailAge_domain_riskLevel",
                "emailAge_domain_riskLevelId",
                "emailAge_eName",
                "emailAge_email_age",
                "emailAge_email_creationDays",
                "emailAge_email_exists",
                "emailAge_email_value",
                "emailAge_firstSeenDays",
                "emailAge_firstVerificationDate",
                "emailAge_fraudType",
                "emailAge_hits_total",
                "emailAge_hits_unique",
                "emailAge_imageUrl",
                "emailAge_lastVerificationDate",
                "emailAge_lastflaggedon",
                "emailAge_location",
                "emailAge_reason_id",
                "emailAge_reason_value",
                "emailAge_riskBand_id",
                "emailAge_riskBand_value",
                "emailAge_score",
                "emailAge_socialMediaFriends",
                "emailAge_sourceIndustry",
                "emailAge_status_id",
                "emailAge_status_value",
                "emailAge_title",
                "metadata_context_traceId",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "emailAge_advice_id",
            "emailAge_advice_value",
            "emailAge_birthDate",
            "emailAge_company",
            "emailAge_country",
            "emailAge_domain_age",
            "emailAge_domain_creationDays",
            "emailAge_domain_exits",
            "emailAge_domain_name",
            "emailAge_domain_riskLevel",
            "emailAge_domain_riskLevelId",
            "emailAge_eName",
            "emailAge_email_age",
            "emailAge_email_creationDays",
            "emailAge_email_exists",
            "emailAge_email_value",
            "emailAge_firstSeenDays",
            "emailAge_firstVerificationDate",
            "emailAge_fraudType",
            "emailAge_hits_total",
            "emailAge_hits_unique",
            "emailAge_imageUrl",
            "emailAge_lastVerificationDate",
            "emailAge_lastflaggedon",
            "emailAge_location",
            "emailAge_reason_id",
            "emailAge_reason_value",
            "emailAge_riskBand_id",
            "emailAge_riskBand_value",
            "emailAge_score",
            "emailAge_socialMediaFriends",
            "emailAge_sourceIndustry",
            "emailAge_status_id",
            "emailAge_status_value",
            "emailAge_title",
            "event_id",
            "metadata_context_traceId",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}