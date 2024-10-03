{% macro return_data_sources_config_braze(model_name) %}

{% set snp_braze_user_attributes = {
    "primary_key": {"expr": "client_id", "alias": "client_id"},
    "sources": [
        {
            "type": "model",
            "name": "dm_target_for_snp_braze_user_attributes_co",
            "where_or_limit_expr": "WHERE COALESCE(ds_to_be_tracked_braze_snapshots,FALSE) AND COALESCE(ae_complies_basic_criteria,FALSE)",
            "columns": [
                {"expr": soft_filter_user_attribute_rules("first_name"),                                               "alias": "first_name"},
                {"expr": soft_filter_user_attribute_rules("last_name"),                                                "alias": "last_name"},
                {"expr": soft_filter_user_attribute_rules("email"),                                                    "alias": "email"},
                {"expr": soft_filter_user_attribute_rules("phone"),                                                    "alias": "phone"},
                {"expr": soft_filter_user_attribute_rules("email_subscribe"),                                          "alias": "email_subscribe"},
                {"expr": soft_filter_user_attribute_rules("push_subscribe"),                                           "alias": "push_subscribe"},
                {"expr": soft_filter_user_attribute_rules("subscription_group_id__marketing_sms__subscription_state"), "alias": "subscription_group_id__marketing_sms__subscription_state"},
                {"expr": soft_filter_user_attribute_rules("subscription_group_id__marketing_wa__subscription_state"),  "alias": "subscription_group_id__marketing_wa__subscription_state"},
                {"expr": soft_filter_user_attribute_rules("age_group"),                                                "alias": "age_group"},
                {"expr": soft_filter_user_attribute_rules("gender"),                                                   "alias": "gender"},
                {"expr": soft_filter_user_attribute_rules("financial_index"),                                          "alias": "financial_index"},
                {"expr": soft_filter_user_attribute_rules("app_index"),                                                "alias": "app_index"},
                {"expr": soft_filter_user_attribute_rules("addi_experience_index"),                                    "alias": "addi_experience_index"},
                {"expr": soft_filter_user_attribute_rules("tech_savvy_index"),                                         "alias": "tech_savvy_index"},
                {"expr": soft_filter_user_attribute_rules("remaining_addicupo_bin"),                                   "alias": "remaining_addicupo_bin"},
                {"expr": soft_filter_user_attribute_rules("used_cupo_bin"),                                            "alias": "used_cupo_bin"},
                {"expr": soft_filter_user_attribute_rules("is_intro"),                                                 "alias": "is_intro"},
                {"expr": soft_filter_user_attribute_rules("is_addi_plus"),                                             "alias": "is_addi_plus"},
                {"expr": soft_filter_user_attribute_rules("is_prospect"),                                              "alias": "is_prospect"},
                {"expr": soft_filter_user_attribute_rules("n_total_purchases"),                                        "alias": "n_total_purchases"},
                {"expr": soft_filter_user_attribute_rules("top_categories"),                                           "alias": "top_categories"},
                {"expr": soft_filter_user_attribute_rules("favorite_category"),                                        "alias": "favorite_category"},
                {"expr": soft_filter_user_attribute_rules("cupo_status"),                                              "alias": "cupo_status"},
                {"expr": soft_filter_user_attribute_rules("weeks_since_last_transaction_bin"),                         "alias": "weeks_since_last_transaction_bin"},
                {"expr": soft_filter_user_attribute_rules("income"),                                                   "alias": "income"},
                {"expr": soft_filter_user_attribute_rules("date_first_purchase"),                                      "alias": "date_first_purchase"},
                {"expr": soft_filter_user_attribute_rules("pap_psl_amount"),                                           "alias": "pap_psl_amount"},
                {"expr": soft_filter_user_attribute_rules("pap_psl_expiration_date"),                                  "alias": "pap_psl_expiration_date"},
                {"expr": soft_filter_user_attribute_rules("pap_psl_segment"),                                          "alias": "pap_psl_segment"},
                {"expr": soft_filter_user_attribute_rules("product_first_loan"),                                       "alias": "product_first_loan"},
                {"expr": soft_filter_user_attribute_rules("reb_cl"),                                                   "alias": "reb_cl"},
                {"expr": soft_filter_user_attribute_rules("test_name"),                                                "alias": "test_name"}
            ]
        }
    ]
} %}

{% set snp_braze_user_attributes_column_types = {
    "primary_key": {"expr": "client_id", "alias": "client_id"},
    "sources": [
        {
            "type": "model",
            "name": "dm_target_for_snp_braze_user_attributes_co",
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
                {"expr": "TYPEOF(age_group)",                                                "alias": "age_group"},
                {"expr": "TYPEOF(gender)",                                                   "alias": "gender"},
                {"expr": "TYPEOF(financial_index)",                                          "alias": "financial_index"},
                {"expr": "TYPEOF(app_index)",                                                "alias": "app_index"},
                {"expr": "TYPEOF(addi_experience_index)",                                    "alias": "addi_experience_index"},
                {"expr": "TYPEOF(tech_savvy_index)",                                         "alias": "tech_savvy_index"},
                {"expr": "TYPEOF(remaining_addicupo_bin)",                                   "alias": "remaining_addicupo_bin"},
                {"expr": "TYPEOF(used_cupo_bin)",                                            "alias": "used_cupo_bin"},
                {"expr": "TYPEOF(is_intro)",                                                 "alias": "is_intro"},
                {"expr": "TYPEOF(is_addi_plus)",                                             "alias": "is_addi_plus"},
                {"expr": "TYPEOF(n_total_purchases)",                                        "alias": "n_total_purchases"},
                {"expr": "TYPEOF(top_categories)",                                           "alias": "top_categories"},
                {"expr": "TYPEOF(favorite_category)",                                        "alias": "favorite_category"},
                {"expr": "TYPEOF(cupo_status)",                                              "alias": "cupo_status"},
                {"expr": "TYPEOF(weeks_since_last_transaction_bin)",                         "alias": "weeks_since_last_transaction_bin"},
                {"expr": "TYPEOF(income)",                                                   "alias": "income"},
                {"expr": "TYPEOF(date_first_purchase)",                                      "alias": "date_first_purchase"},
                {"expr": "TYPEOF(pap_psl_amount)",                                           "alias": "pap_psl_amount"},
                {"expr": "TYPEOF(pap_psl_expiration_date)",                                  "alias": "pap_psl_expiration_date"},
                {"expr": "TYPEOF(pap_psl_segment)",                                          "alias": "pap_psl_segment"},
                {"expr": "TYPEOF(product_first_loan)",                                       "alias": "product_first_loan"},
                {"expr": "TYPEOF(reb_cl)",                                                   "alias": "reb_cl"},
                {"expr": "TYPEOF(test_name)",                                                "alias": "test_name"}
            ]
        }
    ]
} %}

{% set snp_braze_user_deletion = {
    "primary_key": {"expr": "client_id", "alias": "client_id"},
    "sources": [
        {
            "type": "model",
            "name": "dm_target_for_snp_braze_user_deletion_co",
            "where_or_limit_expr": "WHERE COALESCE(ae_complies_basic_criteria,FALSE)",
            "columns": [
                {"expr": "complies_to_be_removed_criteria", "alias": "complies_to_be_removed_criteria"},
                {"expr": "is_in_pii_removal",               "alias": "is_in_pii_removal"},
                {"expr": "has_cupo_status_for_deletion",    "alias": "has_cupo_status_for_deletion"},
            ]
        }
    ]
} %}

{% set snp_braze_user_deletion_column_types = {
    "primary_key": {"expr": "client_id", "alias": "client_id"},
    "sources": [
        {
            "type": "model",
            "name": "dm_target_for_snp_braze_user_deletion_co",
            "where_or_limit_expr": "LIMIT 100",
            "columns": [
                {"expr": "TYPEOF(complies_to_be_removed_criteria)", "alias": "complies_to_be_removed_criteria"},
                {"expr": "TYPEOF(is_in_pii_removal)",               "alias": "is_in_pii_removal"},
                {"expr": "TYPEOF(has_cupo_status_for_deletion)",    "alias": "has_cupo_status_for_deletion"}
            ]
        }
    ]
} %}

{% set snp_braze_purchase_events = {
    "primary_key": {"expr": "application_id", "alias": "application_id"},
    "sources": [
        {
            "type": "model",
            "name": "dm_target_for_snp_braze_purchase_events_co",
            "where_or_limit_expr": "WHERE COALESCE(to_be_tracked_braze_snapshots,FALSE) AND COALESCE(ae_complies_basic_criteria,FALSE)",
            "columns": [
                {"expr": "client_id",                         "alias": "client_id"},
                {"expr": "ally_slug",                         "alias": "ally_slug"},
                {"expr": "origination_datetime_utc",          "alias": "origination_datetime_utc"},
                {"expr": "ally_vertical_lowercase",           "alias": "ally_vertical_lowercase"},
                {"expr": "currency",                          "alias": "currency"},
                {"expr": "gmv_usd",                           "alias": "gmv_usd"},
                {"expr": "synthetic_channel",                 "alias": "synthetic_channel"},
                {"expr": "revenue_proxy_usd",                 "alias": "revenue_proxy_usd"},
                {"expr": "synthetic_product_category",        "alias": "synthetic_product_category"},
                {"expr": "is_addishop_referral_with_default", "alias": "is_addishop_referral_with_default"}
            ]
        }
    ]
} %}

{% set snp_braze_purchase_events_column_types = {
    "primary_key": {"expr": "application_id", "alias": "application_id"},
    "sources": [
        {
            "type": "model",
            "name": "dm_target_for_snp_braze_purchase_events_co",
            "where_or_limit_expr": "LIMIT 100",
            "columns": [
                {"expr": "TYPEOF(client_id)",                         "alias": "client_id"},
                {"expr": "TYPEOF(ally_slug)",                         "alias": "ally_slug"},
                {"expr": "TYPEOF(origination_datetime_utc)",          "alias": "origination_datetime_utc"},
                {"expr": "TYPEOF(ally_vertical_lowercase)",           "alias": "ally_vertical_lowercase"},
                {"expr": "TYPEOF(currency)",                          "alias": "currency"},
                {"expr": "TYPEOF(gmv_usd)",                           "alias": "gmv_usd"},
                {"expr": "TYPEOF(synthetic_channel)",                 "alias": "synthetic_channel"},
                {"expr": "TYPEOF(revenue_proxy_usd)",                 "alias": "revenue_proxy_usd"},
                {"expr": "TYPEOF(synthetic_product_category)",        "alias": "synthetic_product_category"},
                {"expr": "TYPEOF(is_addishop_referral_with_default)", "alias": "is_addishop_referral_with_default"}
            ]
        }
    ]
} %}

{% set snp_braze_tests = {
    "primary_key": {"expr": "test_id", "alias": "test_id"},
    "sources": [
        {
            "type": "source",
            "schema": "sandbox",
            "name": "ae_snapshot_behaviour_test",
            "where_or_limit_expr": "WHERE TRUE",
            "columns": [
                {"expr": "some_string", "alias": "some_string"},
                {"expr": "some_number", "alias": "some_number"},
                {"expr": "some_bool", "alias": "some_bool"},
                {"expr": "some_date", "alias": "some_date"}
            ]
        }
    ]
} %}

{% if model_name == 'snp_braze_user_attributes' %}
    {{ return(snp_braze_user_attributes) }}
{% elif model_name == 'snp_braze_user_attributes_column_types' %}
    {{ return(snp_braze_user_attributes_column_types) }}
{% elif model_name == 'snp_braze_user_deletion' %}
    {{ return(snp_braze_user_deletion) }}
{% elif model_name == 'snp_braze_user_deletion_column_types' %}
    {{ return(snp_braze_user_deletion_column_types) }}
{% elif model_name == 'snp_braze_purchase_events' %}
    {{ return(snp_braze_purchase_events) }}
{% elif model_name == 'snp_braze_purchase_events_column_types' %}
    {{ return(snp_braze_purchase_events_column_types) }}
{% elif model_name == 'snp_braze_tests' %}
    {{ return(snp_braze_tests) }}
{% else %}
    {{ return([]) }}
{% endif %}

{% endmacro %}
