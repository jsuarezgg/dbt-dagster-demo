{% macro return_config_co_f_sanction_list_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_sanction_list_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-06-24 11:24 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_sanction_list_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "sanctionlistmatchdetected_unnested_by_sanctioned_individuals": {
            "direct_attributes": [
                "custom_event_application_santion_list_id_number_pairing_id",
                "event_id",
                "ocurred_on",
                "application_id",
                "client_id",
                "client_type",
                "ally_slug",
                "sanctioned_individuals_country",
                "sanctioned_individuals_full_name",
                "sanctioned_individuals_id_number",
                "sanctioned_individuals_id_type",
                "sanctioned_individuals_list_name"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "custom_event_application_santion_list_id_number_pairing_id",
            "event_id",
            "ocurred_on",
            "application_id",
            "client_id",
            "client_type",
            "ally_slug",
            "sanctioned_individuals_country",
            "sanctioned_individuals_full_name",
            "sanctioned_individuals_id_number",
            "sanctioned_individuals_id_type",
            "sanctioned_individuals_list_name"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}