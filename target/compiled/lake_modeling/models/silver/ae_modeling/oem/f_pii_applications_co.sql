
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
,cellphonealreadylinkedtoadifferentprospectco_co AS ( 
    SELECT *
    FROM bronze.cellphonealreadylinkedtoadifferentprospectco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonealreadylinkedtoexistingclientco_co AS ( 
    SELECT *
    FROM bronze.cellphonealreadylinkedtoexistingclientco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonelistedinblacklistco_co AS ( 
    SELECT *
    FROM bronze.cellphonelistedinblacklistco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,documentlistedinblacklistco_co AS ( 
    SELECT *
    FROM bronze.documentlistedinblacklistco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,emailalreadylinkedtoexistingclientco_co AS ( 
    SELECT *
    FROM bronze.emailalreadylinkedtoexistingclientco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtovalidateemailco_co AS ( 
    SELECT *
    FROM bronze.failedtovalidateemailco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,invalidemailco_co AS ( 
    SELECT *
    FROM bronze.invalidemailco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,listedinofacco_co AS ( 
    SELECT *
    FROM bronze.listedinofacco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectalreadylinkedtodifferentcellphoneco_co AS ( 
    SELECT *
    FROM bronze.prospectalreadylinkedtodifferentcellphoneco_co
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
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM allyapplicationupdated_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM allyisdisabledtooriginateco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM allyisdisabledtooriginatepagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,application_date,application_email,application_id,NULL as campaign_id,channel,client_id,client_is_transactional_based,client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM applicationcreated_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,channel,client_id,NULL as client_is_transactional_based,client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM backgroundcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,client_id,NULL as client_is_transactional_based,client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM checkoutloginsentco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientloanapplicationcreated_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientpreapprovaljourneyisdisabledpagoco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientwaspreapprovedbeforepagoco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckapprovedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM leaddisabledtooriginateco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM leaddisabledtooriginatepagoco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanproposalselectedco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanproposalselectedsantanderco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,preapproval_amount,preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalapplicationcompletedco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preconditionswerevalidco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preconditionswerevalidpagoco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyaccepted_co
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyacceptedco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyacceptedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicysent_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,preapproval_amount,preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectapplicationpreapproved_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,application_channel_legacy,application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectloanapplicationcreatedfromcheckpoint_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,application_channel_legacy,application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectloanapplicationcreatedv2_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectupgradedtoclient_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM requestedamountwasgreaterthanmaximumconfiguredco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM requestedamountwasgreaterthanmaximumconfiguredpagoco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM requestedamountwaslessthanminimumconfiguredco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM requestedamountwaslessthanminimumconfiguredpagoco_co
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM returningclientjourneyisdisableco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM cellphonealreadylinkedtoadifferentprospectco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM cellphonealreadylinkedtoexistingclientco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM cellphonelistedinblacklistco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM documentlistedinblacklistco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM emailalreadylinkedtoexistingclientco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM failedtovalidateemailco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM invalidemailco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM listedinofacco_co
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectalreadylinkedtodifferentcellphoneco_co
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_id,NULL as client_is_transactional_based,NULL as client_type,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_is_santander_branched,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,store_user_id,NULL as store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM backgroundcheckpassed_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,application_cellphone,application_channel_legacy,application_date,application_email,application_id,campaign_id,channel,client_id,client_is_transactional_based,client_type,custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,custom_is_privacy_policy_accepted,custom_is_returning_client_legacy,custom_is_santander_branched,custom_platform_version,id_number,id_type,journey_name,ocurred_on,order_id,preapproval_amount,preapproval_expiration_date,product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_slug,application_cellphone,application_channel_legacy,application_date,application_email,application_id,campaign_id,channel,client_id,client_is_transactional_based,client_type,custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,custom_is_privacy_policy_accepted,custom_is_returning_client_legacy,custom_is_santander_branched,custom_platform_version,id_number,id_type,journey_name,last_event_ocurred_on_processed as ocurred_on,order_id,preapproval_amount,preapproval_expiration_date,product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,store_user_name,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_pii_applications_co  
    WHERE 
    silver.f_pii_applications_co.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN ally_slug is not null then struct(ocurred_on, ally_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_slug as ally_slug,
    element_at(array_sort(array_agg(CASE WHEN application_cellphone is not null then struct(ocurred_on, application_cellphone) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_cellphone as application_cellphone,
    element_at(array_sort(array_agg(CASE WHEN application_channel_legacy is not null then struct(ocurred_on, application_channel_legacy) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_channel_legacy as application_channel_legacy,
    element_at(array_sort(array_agg(CASE WHEN application_date is not null then struct(ocurred_on, application_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_date as application_date,
    element_at(array_sort(array_agg(CASE WHEN application_email is not null then struct(ocurred_on, application_email) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_email as application_email,
    element_at(array_sort(array_agg(CASE WHEN campaign_id is not null then struct(ocurred_on, campaign_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).campaign_id as campaign_id,
    element_at(array_sort(array_agg(CASE WHEN channel is not null then struct(ocurred_on, channel) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).channel as channel,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN client_is_transactional_based is not null then struct(ocurred_on, client_is_transactional_based) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_is_transactional_based as client_is_transactional_based,
    element_at(array_sort(array_agg(CASE WHEN client_type is not null then struct(ocurred_on, client_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_type as client_type,
    element_at(array_sort(array_agg(CASE WHEN custom_is_checkpoint_application_legacy is not null then struct(ocurred_on, custom_is_checkpoint_application_legacy) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_checkpoint_application_legacy as custom_is_checkpoint_application_legacy,
    element_at(array_sort(array_agg(CASE WHEN custom_is_preapproval_completed is not null then struct(ocurred_on, custom_is_preapproval_completed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_preapproval_completed as custom_is_preapproval_completed,
    element_at(array_sort(array_agg(CASE WHEN custom_is_privacy_policy_accepted is not null then struct(ocurred_on, custom_is_privacy_policy_accepted) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_privacy_policy_accepted as custom_is_privacy_policy_accepted,
    element_at(array_sort(array_agg(CASE WHEN custom_is_returning_client_legacy is not null then struct(ocurred_on, custom_is_returning_client_legacy) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_returning_client_legacy as custom_is_returning_client_legacy,
    element_at(array_sort(array_agg(CASE WHEN custom_is_santander_branched is not null then struct(ocurred_on, custom_is_santander_branched) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_santander_branched as custom_is_santander_branched,
    element_at(array_sort(array_agg(CASE WHEN custom_platform_version is not null then struct(ocurred_on, custom_platform_version) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_platform_version as custom_platform_version,
    element_at(array_sort(array_agg(CASE WHEN id_number is not null then struct(ocurred_on, id_number) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_number as id_number,
    element_at(array_sort(array_agg(CASE WHEN id_type is not null then struct(ocurred_on, id_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_type as id_type,
    element_at(array_sort(array_agg(CASE WHEN journey_name is not null then struct(ocurred_on, journey_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).journey_name as journey_name,
    element_at(array_sort(array_agg(CASE WHEN order_id is not null then struct(ocurred_on, order_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).order_id as order_id,
    element_at(array_sort(array_agg(CASE WHEN preapproval_amount is not null then struct(ocurred_on, preapproval_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).preapproval_amount as preapproval_amount,
    element_at(array_sort(array_agg(CASE WHEN preapproval_expiration_date is not null then struct(ocurred_on, preapproval_expiration_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).preapproval_expiration_date as preapproval_expiration_date,
    element_at(array_sort(array_agg(CASE WHEN product is not null then struct(ocurred_on, product) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).product as product,
    element_at(array_sort(array_agg(CASE WHEN requested_amount is not null then struct(ocurred_on, requested_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).requested_amount as requested_amount,
    element_at(array_sort(array_agg(CASE WHEN requested_amount_without_discount is not null then struct(ocurred_on, requested_amount_without_discount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).requested_amount_without_discount as requested_amount_without_discount,
    element_at(array_sort(array_agg(CASE WHEN store_slug is not null then struct(ocurred_on, store_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_slug as store_slug,
    element_at(array_sort(array_agg(CASE WHEN store_user_id is not null then struct(ocurred_on, store_user_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_user_id as store_user_id,
    element_at(array_sort(array_agg(CASE WHEN store_user_name is not null then struct(ocurred_on, store_user_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_user_name as store_user_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    application_id
                       
           )


, final AS (
    SELECT 
        *,
        date(last_event_ocurred_on_processed ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM grouped_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_pii_applications_co
country: co
silver_table_name: f_pii_applications_co
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_cellphone', 'application_channel_legacy', 'application_date', 'application_email', 'application_id', 'campaign_id', 'channel', 'client_id', 'client_is_transactional_based', 'client_type', 'custom_is_checkpoint_application_legacy', 'custom_is_preapproval_completed', 'custom_is_privacy_policy_accepted', 'custom_is_returning_client_legacy', 'custom_is_santander_branched', 'custom_platform_version', 'id_number', 'id_type', 'journey_name', 'ocurred_on', 'order_id', 'preapproval_amount', 'preapproval_expiration_date', 'product', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'store_user_name']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'allyapplicationupdated': {'direct_attributes': ['ally_slug', 'application_id', 'client_id', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'allyisdisabledtooriginateco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'allyisdisabledtooriginatepagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'applicationcreated': {'direct_attributes': ['ally_slug', 'application_cellphone', 'application_date', 'application_email', 'application_id', 'channel', 'client_id', 'client_is_transactional_based', 'client_type', 'custom_platform_version', 'id_number', 'id_type', 'order_id', 'product', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'backgroundcheckpassedco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'checkoutloginsentco': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'clientloanapplicationcreated': {'direct_attributes': ['ally_slug', 'application_cellphone', 'application_date', 'application_email', 'application_id', 'client_id', 'custom_is_returning_client_legacy', 'custom_platform_version', 'id_number', 'id_type', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'clientpreapprovaljourneyisdisabledpagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'clientwaspreapprovedbeforepagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'creditcheckapprovedsantanderco': {'direct_attributes': ['application_id', 'custom_is_santander_branched', 'ocurred_on'], 'custom_attributes': {}}, 'leaddisabledtooriginateco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'leaddisabledtooriginatepagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'loanproposalselectedco': {'direct_attributes': ['application_id', 'campaign_id', 'ocurred_on'], 'custom_attributes': {}}, 'loanproposalselectedsantanderco': {'direct_attributes': ['application_id', 'campaign_id', 'custom_is_santander_branched', 'ocurred_on'], 'custom_attributes': {}}, 'preapprovalapplicationcompletedco': {'direct_attributes': ['application_id', 'custom_is_preapproval_completed', 'preapproval_amount', 'preapproval_expiration_date', 'ocurred_on'], 'custom_attributes': {}}, 'preconditionswerevalidco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'preconditionswerevalidpagoco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicyaccepted': {'direct_attributes': ['application_cellphone', 'application_date', 'application_email', 'application_id', 'client_id', 'custom_is_privacy_policy_accepted', 'id_number', 'id_type', 'requested_amount', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicyacceptedco': {'direct_attributes': ['application_cellphone', 'application_email', 'application_id', 'campaign_id', 'client_id', 'custom_is_privacy_policy_accepted', 'id_number', 'id_type', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicyacceptedsantanderco': {'direct_attributes': ['application_cellphone', 'application_email', 'application_id', 'campaign_id', 'client_id', 'id_number', 'id_type', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicysent': {'direct_attributes': ['ally_slug', 'application_cellphone', 'application_date', 'application_id', 'client_id', 'custom_platform_version', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'prospectapplicationpreapproved': {'direct_attributes': ['application_id', 'custom_is_preapproval_completed', 'preapproval_amount', 'preapproval_expiration_date', 'ocurred_on'], 'custom_attributes': {}}, 'prospectloanapplicationcreatedfromcheckpoint': {'direct_attributes': ['ally_slug', 'application_cellphone', 'application_channel_legacy', 'application_date', 'application_email', 'application_id', 'client_id', 'custom_is_checkpoint_application_legacy', 'custom_is_preapproval_completed', 'custom_platform_version', 'id_number', 'id_type', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'prospectloanapplicationcreatedv2': {'direct_attributes': ['ally_slug', 'application_channel_legacy', 'application_date', 'application_id', 'client_id', 'custom_platform_version', 'order_id', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'store_user_name', 'ocurred_on'], 'custom_attributes': {}}, 'prospectupgradedtoclient': {'direct_attributes': ['application_id', 'application_email', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwasgreaterthanmaximumconfiguredco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwasgreaterthanmaximumconfiguredpagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwaslessthanminimumconfiguredco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwaslessthanminimumconfiguredpagoco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'returningclientjourneyisdisableco': {'direct_attributes': ['application_id', 'ally_slug', 'client_id', 'journey_name', 'ocurred_on'], 'custom_attributes': {}}, 'cellphonealreadylinkedtoadifferentprospectco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'cellphonealreadylinkedtoexistingclientco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'cellphonelistedinblacklistco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'documentlistedinblacklistco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'emailalreadylinkedtoexistingclientco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'failedtovalidateemailco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'invalidemailco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'listedinofacco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'prospectalreadylinkedtodifferentcellphoneco': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'backgroundcheckpassed': {'direct_attributes': ['application_id', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['allyapplicationupdated', 'allyisdisabledtooriginateco', 'allyisdisabledtooriginatepagoco', 'applicationcreated', 'backgroundcheckpassedco', 'checkoutloginsentco', 'clientloanapplicationcreated', 'clientpreapprovaljourneyisdisabledpagoco', 'clientwaspreapprovedbeforepagoco', 'creditcheckapprovedsantanderco', 'leaddisabledtooriginateco', 'leaddisabledtooriginatepagoco', 'loanproposalselectedco', 'loanproposalselectedsantanderco', 'preapprovalapplicationcompletedco', 'preconditionswerevalidco', 'preconditionswerevalidpagoco', 'privacypolicyaccepted', 'privacypolicyacceptedco', 'privacypolicyacceptedsantanderco', 'privacypolicysent', 'prospectapplicationpreapproved', 'prospectloanapplicationcreatedfromcheckpoint', 'prospectloanapplicationcreatedv2', 'prospectupgradedtoclient', 'requestedamountwasgreaterthanmaximumconfiguredco', 'requestedamountwasgreaterthanmaximumconfiguredpagoco', 'requestedamountwaslessthanminimumconfiguredco', 'requestedamountwaslessthanminimumconfiguredpagoco', 'returningclientjourneyisdisableco', 'cellphonealreadylinkedtoadifferentprospectco', 'cellphonealreadylinkedtoexistingclientco', 'cellphonelistedinblacklistco', 'documentlistedinblacklistco', 'emailalreadylinkedtoexistingclientco', 'failedtovalidateemailco', 'invalidemailco', 'listedinofacco', 'prospectalreadylinkedtodifferentcellphoneco', 'backgroundcheckpassed']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
