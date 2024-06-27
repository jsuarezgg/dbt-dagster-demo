{% macro return_config_br_f_loan_cancellations_v2_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_loan_cancellations_v2_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-01-18 12:01 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_loan_cancellations_v2_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "loancancellationorderprocessedv2": {
            "direct_attributes": [
                "event_id",
                "loan_id",
                "ally_slug",
                "client_id",
                "user_id",
                "loan_cancellation_id",
                "loan_cancellation_amount",
                "loan_cancellation_type",
                "loan_cancellation_reason",
                "loan_cancellation_order_date",
                "loan_approved_amount",
                "loan_origination_date",
                "user_role",
                "store_user_name",
                "custom_event_domain",
                "custom_loan_cancellation_status",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loancancellationorderannulledv2": {
            "direct_attributes": [
                "event_id",
                "loan_id",
                "client_id",
                "user_id",
                "loan_cancellation_annulment_reason",
                "loan_cancellation_id",
                "custom_event_domain",
                "custom_loan_cancellation_status",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "client_id",
            "custom_event_domain",
            "custom_loan_cancellation_status",
            "event_id",
            "loan_approved_amount",
            "loan_cancellation_amount",
            "loan_cancellation_annulment_reason",
            "loan_cancellation_id",
            "loan_cancellation_order_date",
            "loan_cancellation_reason",
            "loan_cancellation_type",
            "loan_id",
            "loan_origination_date",
            "ocurred_on",
            "store_user_name",
            "user_id",
            "user_role"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}