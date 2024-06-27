{% macro return_data_sources_config_braze(model_name) %}

{% set snp_braze_user_attributes = {
    "primary_key": {"expr": "client_id", "alias": "client_id"},
    "sources": [
        {
            "type": "model",
            "name": "dm_braze_user_attributes_co",
            "where_or_limit_expr": "WHERE test_name IS NOT NULL",
            "columns": [
                {"expr": "first_name",                                               "alias": "first_name"},
                {"expr": "last_name",                                                "alias": "last_name"},
                {"expr": "email",                                                    "alias": "email"},
                {"expr": "phone",                                                    "alias": "phone"},
                {"expr": "email_subscribe",                                          "alias": "email_subscribe"},
                {"expr": "push_subscribe",                                           "alias": "push_subscribe"},
                {"expr": "subscription_group_id__marketing_sms__subscription_state", "alias": "subscription_group_id__marketing_sms__subscription_state"},
                {"expr": "subscription_group_id__marketing_wa__subscription_state",  "alias": "subscription_group_id__marketing_wa__subscription_state"},
                {"expr": "test_name",                                                "alias": "test_name"},
            ]
        }
    ]
} %}

{% set snp_braze_user_attributes_column_types = {
    "primary_key": {"expr": "client_id", "alias": "client_id"},
    "sources": [
        {
            "type": "model",
            "name": "dm_braze_user_attributes_co",
            "where_or_limit_expr": "LIMIT 100",
            "columns": [
                {"expr": "TYPEOF(first_name)",                                               "alias": "first_name"},
                {"expr": "TYPEOF(last_name)",                                                "alias": "last_name"},
                {"expr": "TYPEOF(email)",                                                    "alias": "email"},
                {"expr": "TYPEOF(phone)",                                                    "alias": "phone"},
                {"expr": "TYPEOF(email_subscribe)",                                          "alias": "email_subscribe"},
                {"expr": "TYPEOF(push_subscribe)",                                           "alias": "push_subscribe"},
                {"expr": "TYPEOF(subscription_group_id__marketing_sms__subscription_state)", "alias": "subscription_group_id__marketing_sms__subscription_state"},
                {"expr": "TYPEOF(subscription_group_id__marketing_wa__subscription_state)",  "alias": "subscription_group_id__marketing_wa__subscription_state"},
                {"expr": "TYPEOF(test_name)",                                                "alias": "test_name"},
            ]
        }
    ]
} %}

{% if model_name == 'snp_braze_user_attributes' %}
    {{ return(snp_braze_user_attributes) }}
{% elif model_name == 'snp_braze_user_attributes_column_types' %}
    {{ return(snp_braze_user_attributes_column_types) }}
{% else %}
    {{ return([]) }}
{% endif %}

{% endmacro %}
