{% macro return_config_br_f_marketplace_transaction_attributable_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_marketplace_transaction_attributable_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-29 16:47 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_marketplace_transaction_attributable_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "TransactionAttributable": {
            "stage": "transaction_attributable_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "shopping_intent_id",
                "ocurred_on",
                "device_id"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "ocurred_on",
            "shopping_intent_id",
            "device_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}