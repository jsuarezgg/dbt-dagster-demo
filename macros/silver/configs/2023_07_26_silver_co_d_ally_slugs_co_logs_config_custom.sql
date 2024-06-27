{% macro return_config_co_d_ally_slugs_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=d_ally_slugs_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-07-26 12:41 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "d_ally_slugs_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "allycreated": {
            "direct_attributes": [
                "event_id",
                "ally_slug",
                "ally_channel",
                "ally_name",
                "ally_state",
                "ally_webpage",
                "ally_address_additional_information",
                "ally_address_city",
                "ally_address_line_one",
                "ally_address_state",
                "ally_financial_information_bank_account",
                "ally_financial_information_bank_account_type",
                "ally_financial_information_bank_code",
                "ally_financial_information_bank_name",
                "ally_financial_information_name",
                "ally_financial_information_number",
                "ally_financial_information_number_check_digit",
                "ally_financial_information_type",
                "ally_legal_identification_check_digit",
                "ally_legal_identification_name",
                "ally_legal_identification_number",
                "ally_legal_identification_tax_payer_type",
                "ally_legal_identification_type",
                "ally_notification_information_email",
                "ally_notification_information_phone",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "allyupdated": {
            "direct_attributes": [
                "event_id",
                "ally_slug",
                "ally_channel",
                "ally_name",
                "ally_state",
                "ally_webpage",
                "ally_address_additional_information",
                "ally_address_city",
                "ally_address_line_one",
                "ally_address_state",
                "ally_financial_information_bank_account",
                "ally_financial_information_bank_account_type",
                "ally_financial_information_bank_code",
                "ally_financial_information_bank_name",
                "ally_financial_information_name",
                "ally_financial_information_number",
                "ally_financial_information_number_check_digit",
                "ally_financial_information_type",
                "ally_legal_identification_check_digit",
                "ally_legal_identification_name",
                "ally_legal_identification_number",
                "ally_legal_identification_tax_payer_type",
                "ally_legal_identification_type",
                "ally_notification_information_email",
                "ally_notification_information_phone",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "allyfinancialinformationupdated": {
            "direct_attributes": [
                "event_id",
                "ally_slug",
                "ally_financial_information_bank_account",
                "ally_financial_information_bank_account_type",
                "ally_financial_information_bank_code",
                "ally_financial_information_bank_name",
                "ally_financial_information_name",
                "ally_financial_information_number",
                "ally_financial_information_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_address_additional_information",
            "ally_address_city",
            "ally_address_line_one",
            "ally_address_state",
            "ally_channel",
            "ally_financial_information_bank_account",
            "ally_financial_information_bank_account_type",
            "ally_financial_information_bank_code",
            "ally_financial_information_bank_name",
            "ally_financial_information_name",
            "ally_financial_information_number",
            "ally_financial_information_number_check_digit",
            "ally_financial_information_type",
            "ally_legal_identification_check_digit",
            "ally_legal_identification_name",
            "ally_legal_identification_number",
            "ally_legal_identification_tax_payer_type",
            "ally_legal_identification_type",
            "ally_name",
            "ally_notification_information_email",
            "ally_notification_information_phone",
            "ally_slug",
            "ally_state",
            "ally_webpage",
            "event_id",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}