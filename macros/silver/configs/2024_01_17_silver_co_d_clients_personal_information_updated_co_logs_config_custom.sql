{% macro return_config_co_d_clients_personal_information_updated_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=d_clients_personal_information_updated_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-01-17 08:51 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "d_clients_personal_information_updated_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "clientcellphoneupdated": {
            "reference_order_id": 1,
            "direct_attributes": [
                "client_id",
                "event_id",
                "ocurred_on",
                "new_cellphone"
            ],
            "custom_attributes": {}
        },
        "clientemailupdated": {
            "reference_order_id": 2,
            "direct_attributes": [
                "client_id",
                "event_id",
                "ocurred_on",
                "new_email"
            ],
            "custom_attributes": {}
        }  
    },
    "unique_db_fields": {
        "direct": [
                "client_id",
                "event_id",
                "ocurred_on",
                "new_email",
                "new_cellphone"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}