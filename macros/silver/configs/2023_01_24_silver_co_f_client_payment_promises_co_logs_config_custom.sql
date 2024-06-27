{% macro return_config_co_f_client_payment_promises_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_client_payment_promises_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-01-25 17:51 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_client_payment_promises_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "clientpaymentpromisegenerated": {
            "direct_attributes": [
                "event_id",
                "payment_promise_id",
                "client_id",
                "state",
                "expected_amount",
                "start_date",
                "end_date",
                "resolution_call",
                "agent_info",
                "agent_code",
                "conditions",
                "ocurred_on",
                "stage"
            ],
            "custom_attributes": {}
        },
        "clientpaymentpromisefulfilled": {
            "direct_attributes": [
                "event_id",
                "payment_promise_id",
                "client_id",
                "state",
                "client_payment_id",
                "ocurred_on",
                "stage"
            ],
            "custom_attributes": {}
        },
        "clientpaymentpromiseunfulfilled": {
            "direct_attributes": [
                "event_id",
                "payment_promise_id",
                "client_id",
                "state",
                "ocurred_on",
                "stage"
            ],
            "custom_attributes": {}
        },
        "clientpaymentpromiseannulled": {
            "direct_attributes": [
                "event_id",
                "payment_promise_id",
                "client_id",
                "state",
                "ocurred_on",
                "stage"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "agent_info",
            "agent_code",
            "client_id",
            "client_payment_id",
            "conditions",
            "end_date",
            "event_id",
            "expected_amount",
            "ocurred_on",
            "payment_promise_id",
            "resolution_call",
            "stage",
            "start_date",
            "state"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}