{% macro return_config_br_d_restricted_entities_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=d_restricted_entities_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-03-13 15:41 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "d_restricted_entities_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "restrictedentitysuspected": {
            "direct_attributes": [
                "event_id",
                "surrogate_key",
                "client_id",
                "restricted_entity_created_at",
                "restricted_entity_journey",
                "restricted_entity_reason",
                "restricted_entity_reference",
                "restricted_entity_source",
                "restricted_entity_status",
                "restricted_entity_type",
                "restricted_entity_value",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "restrictedentityblocked": {
            "direct_attributes": [
                "event_id",
                "surrogate_key",
                "client_id",
                "restricted_entity_created_at",
                "restricted_entity_journey",
                "restricted_entity_reason",
                "restricted_entity_reference",
                "restricted_entity_source",
                "restricted_entity_status",
                "restricted_entity_type",
                "restricted_entity_value",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "restrictedentityactivated": {
            "direct_attributes": [
                "event_id",
                "surrogate_key",
                "client_id",
                "restricted_entity_created_at",
                "restricted_entity_journey",
                "restricted_entity_reason",
                "restricted_entity_reference",
                "restricted_entity_source",
                "restricted_entity_status",
                "restricted_entity_type",
                "restricted_entity_value",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "client_id",
            "event_id",
            "ocurred_on",
            "restricted_entity_created_at",
            "restricted_entity_journey",
            "restricted_entity_reason",
            "restricted_entity_reference",
            "restricted_entity_source",
            "restricted_entity_status",
            "restricted_entity_type",
            "restricted_entity_value",
            "surrogate_key"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}