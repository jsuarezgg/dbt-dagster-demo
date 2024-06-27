{% macro return_config_co_f_fincore_clients_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_fincore_clients_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-08-12 16:16 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_fincore_clients_co",
        "files_db_table_pks": [
            "client_id"
        ]
    },
    "events": {
        "ClientLoansStatusUpdatedV2": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "addicupo_last_update_reason",
                "addicupo_remaining_balance",
                "addicupo_source",
                "addicupo_state",
                "addicupo_total",
                "client_id",
                "delinquency_balance",
                "event_mode",
                "event_version",
                "full_payment",
                "installment_payment",
                "min_payment",
                "ocurred_on",
                "ownership",
                "payday",
                "positive_balance"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "addicupo_last_update_reason",
            "addicupo_remaining_balance",
            "addicupo_source",
            "addicupo_state",
            "addicupo_total",
            "client_id",
            "delinquency_balance",
            "event_mode",
            "event_version",
            "full_payment",
            "installment_payment",
            "min_payment",
            "ocurred_on",
            "ownership",
            "payday",
            "positive_balance"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}