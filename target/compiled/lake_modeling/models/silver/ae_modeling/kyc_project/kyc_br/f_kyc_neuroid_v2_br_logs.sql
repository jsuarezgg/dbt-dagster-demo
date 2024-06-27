
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
neuroidobtained_v2_br AS ( 
    SELECT *
    FROM bronze.neuroidobtained_v2_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,metadata_context_traceId,neuroId_cellphoneNumber_autofill,neuroId_cellphoneNumber_dataImport,neuroId_cellphoneNumber_frictionIndex,neuroId_cellphoneNumber_hesitation,neuroId_cellphoneNumber_interactionTime,neuroId_cellphoneNumber_manipulation,neuroId_cellphoneNumber_repeatInteractions,neuroId_cellphoneNumber_timeToFirstAnswer,neuroId_email_autofill,neuroId_email_dataImport,neuroId_email_frictionIndex,neuroId_email_hesitation,neuroId_email_interactionTime,neuroId_email_manipulation,neuroId_email_repeatInteractions,neuroId_email_timeToFirstAnswer,neuroId_fullName_autofill,neuroId_fullName_dataImport,neuroId_fullName_frictionIndex,neuroId_fullName_hesitation,neuroId_fullName_interactionTime,neuroId_fullName_manipulation,neuroId_fullName_repeatInteractions,neuroId_fullName_timeToFirstAnswer,neuroId_nationalIdentificationNumber_autofill,neuroId_nationalIdentificationNumber_dataImport,neuroId_nationalIdentificationNumber_frictionIndex,neuroId_nationalIdentificationNumber_hesitation,neuroId_nationalIdentificationNumber_interactionTime,neuroId_nationalIdentificationNumber_manipulation,neuroId_nationalIdentificationNumber_repeatInteractions,neuroId_nationalIdentificationNumber_timeToFirstAnswer,neuroId_nationality_autofill,neuroId_nationality_dataImport,neuroId_nationality_frictionIndex,neuroId_nationality_hesitation,neuroId_nationality_interactionTime,neuroId_nationality_manipulation,neuroId_nationality_repeatInteractions,neuroId_nationality_timeToFirstAnswer,neuroId_otpInput0_autofill,neuroId_otpInput0_dataImport,neuroId_otpInput0_frictionIndex,neuroId_otpInput0_hesitation,neuroId_otpInput0_interactionTime,neuroId_otpInput0_manipulation,neuroId_otpInput0_repeatInteractions,neuroId_otpInput0_timeToFirstAnswer,neuroId_otpInput1_autofill,neuroId_otpInput1_dataImport,neuroId_otpInput1_frictionIndex,neuroId_otpInput1_hesitation,neuroId_otpInput1_interactionTime,neuroId_otpInput1_manipulation,neuroId_otpInput1_repeatInteractions,neuroId_otpInput1_timeToFirstAnswer,neuroId_otpInput2_autofill,neuroId_otpInput2_dataImport,neuroId_otpInput2_frictionIndex,neuroId_otpInput2_hesitation,neuroId_otpInput2_interactionTime,neuroId_otpInput2_manipulation,neuroId_otpInput2_repeatInteractions,neuroId_otpInput2_timeToFirstAnswer,neuroId_otpInput3_autofill,neuroId_otpInput3_dataImport,neuroId_otpInput3_frictionIndex,neuroId_otpInput3_hesitation,neuroId_otpInput3_interactionTime,neuroId_otpInput3_manipulation,neuroId_otpInput3_repeatInteractions,neuroId_otpInput3_timeToFirstAnswer,neuroId_otpInput4_autofill,neuroId_otpInput4_dataImport,neuroId_otpInput4_frictionIndex,neuroId_otpInput4_hesitation,neuroId_otpInput4_interactionTime,neuroId_otpInput4_manipulation,neuroId_otpInput4_repeatInteractions,neuroId_otpInput4_timeToFirstAnswer,neuroId_otpInput5_autofill,neuroId_otpInput5_dataImport,neuroId_otpInput5_frictionIndex,neuroId_otpInput5_hesitation,neuroId_otpInput5_interactionTime,neuroId_otpInput5_manipulation,neuroId_otpInput5_repeatInteractions,neuroId_otpInput5_timeToFirstAnswer,neuroId_sessionBreaks,neuroId_totalSessionIdleTime,neuroId_totalSessionInteractionTime,ocurred_on,
    event_name,
    event_id
    FROM neuroidobtained_v2_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,metadata_context_traceId,neuroId_cellphoneNumber_autofill,neuroId_cellphoneNumber_dataImport,neuroId_cellphoneNumber_frictionIndex,neuroId_cellphoneNumber_hesitation,neuroId_cellphoneNumber_interactionTime,neuroId_cellphoneNumber_manipulation,neuroId_cellphoneNumber_repeatInteractions,neuroId_cellphoneNumber_timeToFirstAnswer,neuroId_email_autofill,neuroId_email_dataImport,neuroId_email_frictionIndex,neuroId_email_hesitation,neuroId_email_interactionTime,neuroId_email_manipulation,neuroId_email_repeatInteractions,neuroId_email_timeToFirstAnswer,neuroId_fullName_autofill,neuroId_fullName_dataImport,neuroId_fullName_frictionIndex,neuroId_fullName_hesitation,neuroId_fullName_interactionTime,neuroId_fullName_manipulation,neuroId_fullName_repeatInteractions,neuroId_fullName_timeToFirstAnswer,neuroId_nationalIdentificationNumber_autofill,neuroId_nationalIdentificationNumber_dataImport,neuroId_nationalIdentificationNumber_frictionIndex,neuroId_nationalIdentificationNumber_hesitation,neuroId_nationalIdentificationNumber_interactionTime,neuroId_nationalIdentificationNumber_manipulation,neuroId_nationalIdentificationNumber_repeatInteractions,neuroId_nationalIdentificationNumber_timeToFirstAnswer,neuroId_nationality_autofill,neuroId_nationality_dataImport,neuroId_nationality_frictionIndex,neuroId_nationality_hesitation,neuroId_nationality_interactionTime,neuroId_nationality_manipulation,neuroId_nationality_repeatInteractions,neuroId_nationality_timeToFirstAnswer,neuroId_otpInput0_autofill,neuroId_otpInput0_dataImport,neuroId_otpInput0_frictionIndex,neuroId_otpInput0_hesitation,neuroId_otpInput0_interactionTime,neuroId_otpInput0_manipulation,neuroId_otpInput0_repeatInteractions,neuroId_otpInput0_timeToFirstAnswer,neuroId_otpInput1_autofill,neuroId_otpInput1_dataImport,neuroId_otpInput1_frictionIndex,neuroId_otpInput1_hesitation,neuroId_otpInput1_interactionTime,neuroId_otpInput1_manipulation,neuroId_otpInput1_repeatInteractions,neuroId_otpInput1_timeToFirstAnswer,neuroId_otpInput2_autofill,neuroId_otpInput2_dataImport,neuroId_otpInput2_frictionIndex,neuroId_otpInput2_hesitation,neuroId_otpInput2_interactionTime,neuroId_otpInput2_manipulation,neuroId_otpInput2_repeatInteractions,neuroId_otpInput2_timeToFirstAnswer,neuroId_otpInput3_autofill,neuroId_otpInput3_dataImport,neuroId_otpInput3_frictionIndex,neuroId_otpInput3_hesitation,neuroId_otpInput3_interactionTime,neuroId_otpInput3_manipulation,neuroId_otpInput3_repeatInteractions,neuroId_otpInput3_timeToFirstAnswer,neuroId_otpInput4_autofill,neuroId_otpInput4_dataImport,neuroId_otpInput4_frictionIndex,neuroId_otpInput4_hesitation,neuroId_otpInput4_interactionTime,neuroId_otpInput4_manipulation,neuroId_otpInput4_repeatInteractions,neuroId_otpInput4_timeToFirstAnswer,neuroId_otpInput5_autofill,neuroId_otpInput5_dataImport,neuroId_otpInput5_frictionIndex,neuroId_otpInput5_hesitation,neuroId_otpInput5_interactionTime,neuroId_otpInput5_manipulation,neuroId_otpInput5_repeatInteractions,neuroId_otpInput5_timeToFirstAnswer,neuroId_sessionBreaks,neuroId_totalSessionIdleTime,neuroId_totalSessionInteractionTime,ocurred_on,
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
this: silver.f_kyc_neuroid_v2_br_logs
country: br
silver_table_name: f_kyc_neuroid_v2_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'event_id', 'metadata_context_traceId', 'neuroId_cellphoneNumber_autofill', 'neuroId_cellphoneNumber_dataImport', 'neuroId_cellphoneNumber_frictionIndex', 'neuroId_cellphoneNumber_hesitation', 'neuroId_cellphoneNumber_interactionTime', 'neuroId_cellphoneNumber_manipulation', 'neuroId_cellphoneNumber_repeatInteractions', 'neuroId_cellphoneNumber_timeToFirstAnswer', 'neuroId_email_autofill', 'neuroId_email_dataImport', 'neuroId_email_frictionIndex', 'neuroId_email_hesitation', 'neuroId_email_interactionTime', 'neuroId_email_manipulation', 'neuroId_email_repeatInteractions', 'neuroId_email_timeToFirstAnswer', 'neuroId_fullName_autofill', 'neuroId_fullName_dataImport', 'neuroId_fullName_frictionIndex', 'neuroId_fullName_hesitation', 'neuroId_fullName_interactionTime', 'neuroId_fullName_manipulation', 'neuroId_fullName_repeatInteractions', 'neuroId_fullName_timeToFirstAnswer', 'neuroId_nationalIdentificationNumber_autofill', 'neuroId_nationalIdentificationNumber_dataImport', 'neuroId_nationalIdentificationNumber_frictionIndex', 'neuroId_nationalIdentificationNumber_hesitation', 'neuroId_nationalIdentificationNumber_interactionTime', 'neuroId_nationalIdentificationNumber_manipulation', 'neuroId_nationalIdentificationNumber_repeatInteractions', 'neuroId_nationalIdentificationNumber_timeToFirstAnswer', 'neuroId_nationality_autofill', 'neuroId_nationality_dataImport', 'neuroId_nationality_frictionIndex', 'neuroId_nationality_hesitation', 'neuroId_nationality_interactionTime', 'neuroId_nationality_manipulation', 'neuroId_nationality_repeatInteractions', 'neuroId_nationality_timeToFirstAnswer', 'neuroId_otpInput0_autofill', 'neuroId_otpInput0_dataImport', 'neuroId_otpInput0_frictionIndex', 'neuroId_otpInput0_hesitation', 'neuroId_otpInput0_interactionTime', 'neuroId_otpInput0_manipulation', 'neuroId_otpInput0_repeatInteractions', 'neuroId_otpInput0_timeToFirstAnswer', 'neuroId_otpInput1_autofill', 'neuroId_otpInput1_dataImport', 'neuroId_otpInput1_frictionIndex', 'neuroId_otpInput1_hesitation', 'neuroId_otpInput1_interactionTime', 'neuroId_otpInput1_manipulation', 'neuroId_otpInput1_repeatInteractions', 'neuroId_otpInput1_timeToFirstAnswer', 'neuroId_otpInput2_autofill', 'neuroId_otpInput2_dataImport', 'neuroId_otpInput2_frictionIndex', 'neuroId_otpInput2_hesitation', 'neuroId_otpInput2_interactionTime', 'neuroId_otpInput2_manipulation', 'neuroId_otpInput2_repeatInteractions', 'neuroId_otpInput2_timeToFirstAnswer', 'neuroId_otpInput3_autofill', 'neuroId_otpInput3_dataImport', 'neuroId_otpInput3_frictionIndex', 'neuroId_otpInput3_hesitation', 'neuroId_otpInput3_interactionTime', 'neuroId_otpInput3_manipulation', 'neuroId_otpInput3_repeatInteractions', 'neuroId_otpInput3_timeToFirstAnswer', 'neuroId_otpInput4_autofill', 'neuroId_otpInput4_dataImport', 'neuroId_otpInput4_frictionIndex', 'neuroId_otpInput4_hesitation', 'neuroId_otpInput4_interactionTime', 'neuroId_otpInput4_manipulation', 'neuroId_otpInput4_repeatInteractions', 'neuroId_otpInput4_timeToFirstAnswer', 'neuroId_otpInput5_autofill', 'neuroId_otpInput5_dataImport', 'neuroId_otpInput5_frictionIndex', 'neuroId_otpInput5_hesitation', 'neuroId_otpInput5_interactionTime', 'neuroId_otpInput5_manipulation', 'neuroId_otpInput5_repeatInteractions', 'neuroId_otpInput5_timeToFirstAnswer', 'neuroId_sessionBreaks', 'neuroId_totalSessionIdleTime', 'neuroId_totalSessionInteractionTime', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'neuroidobtained_v2': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'neuroId_cellphoneNumber_autofill', 'neuroId_cellphoneNumber_dataImport', 'neuroId_cellphoneNumber_frictionIndex', 'neuroId_cellphoneNumber_hesitation', 'neuroId_cellphoneNumber_interactionTime', 'neuroId_cellphoneNumber_manipulation', 'neuroId_cellphoneNumber_repeatInteractions', 'neuroId_cellphoneNumber_timeToFirstAnswer', 'neuroId_email_autofill', 'neuroId_email_dataImport', 'neuroId_email_frictionIndex', 'neuroId_email_hesitation', 'neuroId_email_interactionTime', 'neuroId_email_manipulation', 'neuroId_email_repeatInteractions', 'neuroId_email_timeToFirstAnswer', 'neuroId_fullName_autofill', 'neuroId_fullName_dataImport', 'neuroId_fullName_frictionIndex', 'neuroId_fullName_hesitation', 'neuroId_fullName_interactionTime', 'neuroId_fullName_manipulation', 'neuroId_fullName_repeatInteractions', 'neuroId_fullName_timeToFirstAnswer', 'neuroId_nationalIdentificationNumber_autofill', 'neuroId_nationalIdentificationNumber_dataImport', 'neuroId_nationalIdentificationNumber_frictionIndex', 'neuroId_nationalIdentificationNumber_hesitation', 'neuroId_nationalIdentificationNumber_interactionTime', 'neuroId_nationalIdentificationNumber_manipulation', 'neuroId_nationalIdentificationNumber_repeatInteractions', 'neuroId_nationalIdentificationNumber_timeToFirstAnswer', 'neuroId_nationality_autofill', 'neuroId_nationality_dataImport', 'neuroId_nationality_frictionIndex', 'neuroId_nationality_hesitation', 'neuroId_nationality_interactionTime', 'neuroId_nationality_manipulation', 'neuroId_nationality_repeatInteractions', 'neuroId_nationality_timeToFirstAnswer', 'neuroId_otpInput0_autofill', 'neuroId_otpInput0_dataImport', 'neuroId_otpInput0_frictionIndex', 'neuroId_otpInput0_hesitation', 'neuroId_otpInput0_interactionTime', 'neuroId_otpInput0_manipulation', 'neuroId_otpInput0_repeatInteractions', 'neuroId_otpInput0_timeToFirstAnswer', 'neuroId_otpInput1_autofill', 'neuroId_otpInput1_dataImport', 'neuroId_otpInput1_frictionIndex', 'neuroId_otpInput1_hesitation', 'neuroId_otpInput1_interactionTime', 'neuroId_otpInput1_manipulation', 'neuroId_otpInput1_repeatInteractions', 'neuroId_otpInput1_timeToFirstAnswer', 'neuroId_otpInput2_autofill', 'neuroId_otpInput2_dataImport', 'neuroId_otpInput2_frictionIndex', 'neuroId_otpInput2_hesitation', 'neuroId_otpInput2_interactionTime', 'neuroId_otpInput2_manipulation', 'neuroId_otpInput2_repeatInteractions', 'neuroId_otpInput2_timeToFirstAnswer', 'neuroId_otpInput3_autofill', 'neuroId_otpInput3_dataImport', 'neuroId_otpInput3_frictionIndex', 'neuroId_otpInput3_hesitation', 'neuroId_otpInput3_interactionTime', 'neuroId_otpInput3_manipulation', 'neuroId_otpInput3_repeatInteractions', 'neuroId_otpInput3_timeToFirstAnswer', 'neuroId_otpInput4_autofill', 'neuroId_otpInput4_dataImport', 'neuroId_otpInput4_frictionIndex', 'neuroId_otpInput4_hesitation', 'neuroId_otpInput4_interactionTime', 'neuroId_otpInput4_manipulation', 'neuroId_otpInput4_repeatInteractions', 'neuroId_otpInput4_timeToFirstAnswer', 'neuroId_otpInput5_autofill', 'neuroId_otpInput5_dataImport', 'neuroId_otpInput5_frictionIndex', 'neuroId_otpInput5_hesitation', 'neuroId_otpInput5_interactionTime', 'neuroId_otpInput5_manipulation', 'neuroId_otpInput5_repeatInteractions', 'neuroId_otpInput5_timeToFirstAnswer', 'neuroId_sessionBreaks', 'neuroId_totalSessionIdleTime', 'neuroId_totalSessionInteractionTime', 'metadata_context_traceId', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['neuroidobtained_v2']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
