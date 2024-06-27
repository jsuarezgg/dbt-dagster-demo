
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
allyapplicationupdated_co AS ( 
    SELECT *
    FROM bronze.allyapplicationupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,allyisdisabledtooriginateco_co AS ( 
    SELECT *
    FROM bronze.allyisdisabledtooriginateco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,allyisdisabledtooriginatepagoco_co AS ( 
    SELECT *
    FROM bronze.allyisdisabledtooriginatepagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationcreated_co AS ( 
    SELECT *
    FROM bronze.applicationcreated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,backgroundcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.backgroundcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginsentco_co AS ( 
    SELECT *
    FROM bronze.checkoutloginsentco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientloanapplicationcreated_co AS ( 
    SELECT *
    FROM bronze.clientloanapplicationcreated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpreapprovaljourneyisdisabledpagoco_co AS ( 
    SELECT *
    FROM bronze.clientpreapprovaljourneyisdisabledpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientwaspreapprovedbeforepagoco_co AS ( 
    SELECT *
    FROM bronze.clientwaspreapprovedbeforepagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckapprovedsantanderco_co AS ( 
    SELECT *
    FROM bronze.creditcheckapprovedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,leaddisabledtooriginateco_co AS ( 
    SELECT *
    FROM bronze.leaddisabledtooriginateco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,leaddisabledtooriginatepagoco_co AS ( 
    SELECT *
    FROM bronze.leaddisabledtooriginatepagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalselectedco_co AS ( 
    SELECT *
    FROM bronze.loanproposalselectedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalselectedsantanderco_co AS ( 
    SELECT *
    FROM bronze.loanproposalselectedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalapplicationcompletedco_co AS ( 
    SELECT *
    FROM bronze.preapprovalapplicationcompletedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preconditionswerevalidco_co AS ( 
    SELECT *
    FROM bronze.preconditionswerevalidco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preconditionswerevalidpagoco_co AS ( 
    SELECT *
    FROM bronze.preconditionswerevalidpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyaccepted_co AS ( 
    SELECT *
    FROM bronze.privacypolicyaccepted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedco_co AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedsantanderco_co AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicysent_co AS ( 
    SELECT *
    FROM bronze.privacypolicysent_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectapplicationpreapproved_co AS ( 
    SELECT *
    FROM bronze.prospectapplicationpreapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectloanapplicationcreatedfromcheckpoint_co AS ( 
    SELECT *
    FROM bronze.prospectloanapplicationcreatedfromcheckpoint_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectloanapplicationcreatedv2_co AS ( 
    SELECT *
    FROM bronze.prospectloanapplicationcreatedv2_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectupgradedtoclient_co AS ( 
    SELECT *
    FROM bronze.prospectupgradedtoclient_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwasgreaterthanmaximumconfiguredco_co AS ( 
    SELECT *
    FROM bronze.requestedamountwasgreaterthanmaximumconfiguredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwasgreaterthanmaximumconfiguredpagoco_co AS ( 
    SELECT *
    FROM bronze.requestedamountwasgreaterthanmaximumconfiguredpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwaslessthanminimumconfiguredco_co AS ( 
    SELECT *
    FROM bronze.requestedamountwaslessthanminimumconfiguredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwaslessthanminimumconfiguredpagoco_co AS ( 
    SELECT *
    FROM bronze.requestedamountwaslessthanminimumconfiguredpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,returningclientjourneyisdisableco_co AS ( 
    SELECT *
    FROM bronze.returningclientjourneyisdisableco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,backgroundcheckpassed_co AS ( 
    SELECT *
    FROM bronze.backgroundcheckpassed_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM allyapplicationupdated_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM allyisdisabledtooriginateco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM allyisdisabledtooriginatepagoco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,application_date,application_id,NULL as campaign_id,channel,client_id,client_is_transactional_based,client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,NULL as journey_name,ocurred_on,order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM applicationcreated_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,channel,client_id,NULL as client_is_transactional_based,client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM backgroundcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,channel,client_id,NULL as client_is_transactional_based,client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM checkoutloginsentco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,
    event_name,
    event_id
    FROM clientloanapplicationcreated_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM clientpreapprovaljourneyisdisabledpagoco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM clientwaspreapprovedbeforepagoco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM creditcheckapprovedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM leaddisabledtooriginateco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM leaddisabledtooriginatepagoco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM loanproposalselectedco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM loanproposalselectedsantanderco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,preapproval_amount,preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM preapprovalapplicationcompletedco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM preconditionswerevalidco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM preconditionswerevalidpagoco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,store_user_name,
    event_name,
    event_id
    FROM privacypolicyaccepted_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM privacypolicyacceptedco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM privacypolicyacceptedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,store_user_name,
    event_name,
    event_id
    FROM privacypolicysent_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,preapproval_amount,preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM prospectapplicationpreapproved_co
    UNION ALL
    SELECT 
        ally_slug,application_channel_legacy,application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,
    event_name,
    event_id
    FROM prospectloanapplicationcreatedfromcheckpoint_co
    UNION ALL
    SELECT 
        ally_slug,application_channel_legacy,application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,NULL as journey_name,ocurred_on,order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,
    event_name,
    event_id
    FROM prospectloanapplicationcreatedv2_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM prospectupgradedtoclient_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM requestedamountwasgreaterthanmaximumconfiguredco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM requestedamountwasgreaterthanmaximumconfiguredpagoco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM requestedamountwaslessthanminimumconfiguredco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM requestedamountwaslessthanminimumconfiguredpagoco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM returningclientjourneyisdisableco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_channel_legacy,NULL as application_date,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,store_user_id,NULL as store_user_name,
    event_name,
    event_id
    FROM backgroundcheckpassed_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,application_channel_legacy,application_date,application_id,campaign_id,channel,client_id,client_is_transactional_based,client_type,custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,custom_is_privacy_policy_accepted,custom_is_returning_client_legacy,custom_is_santander_branched,custom_platform_version,journey_name,ocurred_on,order_id,preapproval_amount,preapproval_expiration_date,product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,
    event_name,
    event_id
    FROM union_bronze 
    
)   



, final AS (
    SELECT 
        *,
        date(ocurred_on ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM union_all_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_applications_co_logs
country: co
silver_table_name: f_applications_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_channel_legacy', 'application_date', 'application_id', 'campaign_id', 'channel', 'client_id', 'client_is_transactional_based', 'client_type', 'custom_is_checkpoint_application_legacy', 'custom_is_preapproval_completed', 'custom_is_privacy_policy_accepted', 'custom_is_returning_client_legacy', 'custom_is_santander_branched', 'custom_platform_version', 'event_id', 'journey_name', 'ocurred_on', 'order_id', 'preapproval_amount', 'preapproval_expiration_date', 'product', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'store_user_name']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'allyapplicationupdated': {'direct_attributes': ['application_id', 'ally_slug', 'event_id', 'client_id', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'allyisdisabledtooriginateco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'allyisdisabledtooriginatepagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'applicationcreated': {'direct_attributes': ['application_id', 'ally_slug', 'application_date', 'event_id', 'channel', 'client_id', 'client_is_transactional_based', 'client_type', 'custom_platform_version', 'order_id', 'product', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'backgroundcheckpassedco': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'event_id', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'checkoutloginsentco': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'event_id', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'clientloanapplicationcreated': {'direct_attributes': ['application_id', 'ally_slug', 'application_date', 'event_id', 'client_id', 'custom_is_returning_client_legacy', 'custom_platform_version', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'clientpreapprovaljourneyisdisabledpagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'clientwaspreapprovedbeforepagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'creditcheckapprovedsantanderco': {'direct_attributes': ['application_id', 'event_id', 'custom_is_santander_branched', 'ocurred_on'], 'custom_attributes': {}}, 'leaddisabledtooriginateco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'leaddisabledtooriginatepagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'loanproposalselectedco': {'direct_attributes': ['application_id', 'campaign_id', 'event_id', 'ocurred_on'], 'custom_attributes': {}}, 'loanproposalselectedsantanderco': {'direct_attributes': ['application_id', 'campaign_id', 'event_id', 'custom_is_santander_branched', 'ocurred_on'], 'custom_attributes': {}}, 'preapprovalapplicationcompletedco': {'direct_attributes': ['application_id', 'custom_is_preapproval_completed', 'event_id', 'preapproval_amount', 'preapproval_expiration_date', 'ocurred_on'], 'custom_attributes': {}}, 'preconditionswerevalidco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'preconditionswerevalidpagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicyaccepted': {'direct_attributes': ['application_id', 'application_date', 'event_id', 'client_id', 'custom_is_privacy_policy_accepted', 'requested_amount', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicyacceptedco': {'direct_attributes': ['application_id', 'event_id', 'campaign_id', 'client_id', 'custom_is_privacy_policy_accepted', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicyacceptedsantanderco': {'direct_attributes': ['application_id', 'ally_slug', 'event_id', 'campaign_id', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicysent': {'direct_attributes': ['application_id', 'ally_slug', 'application_date', 'event_id', 'client_id', 'custom_platform_version', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'prospectapplicationpreapproved': {'direct_attributes': ['application_id', 'custom_is_preapproval_completed', 'event_id', 'preapproval_amount', 'preapproval_expiration_date', 'ocurred_on'], 'custom_attributes': {}}, 'prospectloanapplicationcreatedfromcheckpoint': {'direct_attributes': ['application_id', 'ally_slug', 'application_channel_legacy', 'application_date', 'event_id', 'client_id', 'custom_is_checkpoint_application_legacy', 'custom_is_preapproval_completed', 'custom_platform_version', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'prospectloanapplicationcreatedv2': {'direct_attributes': ['application_id', 'ally_slug', 'application_channel_legacy', 'application_date', 'event_id', 'client_id', 'custom_platform_version', 'order_id', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'prospectupgradedtoclient': {'direct_attributes': ['application_id', 'event_id', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwasgreaterthanmaximumconfiguredco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwasgreaterthanmaximumconfiguredpagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwaslessthanminimumconfiguredco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwaslessthanminimumconfiguredpagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'returningclientjourneyisdisableco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'event_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'backgroundcheckpassed': {'direct_attributes': ['application_id', 'event_id', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['allyapplicationupdated', 'allyisdisabledtooriginateco', 'allyisdisabledtooriginatepagoco', 'applicationcreated', 'backgroundcheckpassedco', 'checkoutloginsentco', 'clientloanapplicationcreated', 'clientpreapprovaljourneyisdisabledpagoco', 'clientwaspreapprovedbeforepagoco', 'creditcheckapprovedsantanderco', 'leaddisabledtooriginateco', 'leaddisabledtooriginatepagoco', 'loanproposalselectedco', 'loanproposalselectedsantanderco', 'preapprovalapplicationcompletedco', 'preconditionswerevalidco', 'preconditionswerevalidpagoco', 'privacypolicyaccepted', 'privacypolicyacceptedco', 'privacypolicyacceptedsantanderco', 'privacypolicysent', 'prospectapplicationpreapproved', 'prospectloanapplicationcreatedfromcheckpoint', 'prospectloanapplicationcreatedv2', 'prospectupgradedtoclient', 'requestedamountwasgreaterthanmaximumconfiguredco', 'requestedamountwasgreaterthanmaximumconfiguredpagoco', 'requestedamountwaslessthanminimumconfiguredco', 'requestedamountwaslessthanminimumconfiguredpagoco', 'returningclientjourneyisdisableco', 'backgroundcheckpassed']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
