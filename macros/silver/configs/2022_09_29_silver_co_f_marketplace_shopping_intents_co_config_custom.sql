{% macro return_config_co_f_marketplace_shopping_intents_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_marketplace_shopping_intents_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-29 16:47 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_marketplace_shopping_intents_co",
        "files_db_table_pks": [
            "shopping_intent_id"
        ]
    },
    "events": {
        "ShoppingIntentRegistered": {
            "stage": "shoppingintentregistered_co",
            "direct_attributes": [
                "ally_slug",
                "shopping_intent_id",
                "category",
                "subcategory",
                "screen",
                "component",
                "client_id",
                "campaign_id",
                "channel",
                "ocurred_on",
                "device_id",
                "shopping_intent_timestamp"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "campaign_id",
            "channel",
            "client_id",
            "ocurred_on",
            "shopping_intent_id",
            "category",
            "subcategory",
            "screen",
            "component",
            "device_id",
            "shopping_intent_timestamp"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}