{% macro return_config_co_f_applications_addi_v2_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_addi_v2_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-11-14 16:49 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_addi_v2_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "clientcreditcheckfailedpagoco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientcreditcheckpassedpagoco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "conditionalcreditcheckpassedpagoco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "creditcheckfailedpagoco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "creditcheckpassedpagoco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "creditcheckpassedwithoutloan": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosstarted": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanacceptedco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "conditionalclientcreditcheckpassedpagoco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanrefinanceproposalscreatedco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "refinanceloanacceptedco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "product_v2",
                "evaluation_type",
                "event_type",
                "journey_name",
                "journey_stage_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "evaluation_type",
            "event_id",
            "event_type",
            "journey_name",
            "journey_stage_name",
            "ocurred_on",
            "product_v2"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}