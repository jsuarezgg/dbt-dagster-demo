{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--raw_modeling.neuroidobtained_v2_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.data.cellphoneNumber.autofill AS neuroId_cellphoneNumber_autofill,
    json_tmp.data.cellphoneNumber.dataImport AS neuroId_cellphoneNumber_dataImport,
    json_tmp.data.cellphoneNumber.frictionIndex AS neuroId_cellphoneNumber_frictionIndex,
    json_tmp.data.cellphoneNumber.hesitation AS neuroId_cellphoneNumber_hesitation,
    json_tmp.data.cellphoneNumber.interactionTime AS neuroId_cellphoneNumber_interactionTime,
    json_tmp.data.cellphoneNumber.manipulation AS neuroId_cellphoneNumber_manipulation,
    json_tmp.data.cellphoneNumber.repeatInteractions AS neuroId_cellphoneNumber_repeatInteractions,
    json_tmp.data.cellphoneNumber.timeToFirstAnswer AS neuroId_cellphoneNumber_timeToFirstAnswer,
    json_tmp.data.email.autofill AS neuroId_email_autofill,
    json_tmp.data.email.dataImport AS neuroId_email_dataImport,
    json_tmp.data.email.frictionIndex AS neuroId_email_frictionIndex,
    json_tmp.data.email.hesitation AS neuroId_email_hesitation,
    json_tmp.data.email.interactionTime AS neuroId_email_interactionTime,
    json_tmp.data.email.manipulation AS neuroId_email_manipulation,
    json_tmp.data.email.repeatInteractions AS neuroId_email_repeatInteractions,
    json_tmp.data.email.timeToFirstAnswer AS neuroId_email_timeToFirstAnswer,
    json_tmp.data.fullName.autofill AS neuroId_fullName_autofill,
    json_tmp.data.fullName.dataImport AS neuroId_fullName_dataImport,
    json_tmp.data.fullName.frictionIndex AS neuroId_fullName_frictionIndex,
    json_tmp.data.fullName.hesitation AS neuroId_fullName_hesitation,
    json_tmp.data.fullName.interactionTime AS neuroId_fullName_interactionTime,
    json_tmp.data.fullName.manipulation AS neuroId_fullName_manipulation,
    json_tmp.data.fullName.repeatInteractions AS neuroId_fullName_repeatInteractions,
    json_tmp.data.fullName.timeToFirstAnswer AS neuroId_fullName_timeToFirstAnswer,
    json_tmp.data.nationalIdentificationNumber.autofill AS neuroId_nationalIdentificationNumber_autofill,
    json_tmp.data.nationalIdentificationNumber.dataImport AS neuroId_nationalIdentificationNumber_dataImport,
    json_tmp.data.nationalIdentificationNumber.frictionIndex AS neuroId_nationalIdentificationNumber_frictionIndex,
    json_tmp.data.nationalIdentificationNumber.hesitation AS neuroId_nationalIdentificationNumber_hesitation,
    json_tmp.data.nationalIdentificationNumber.interactionTime AS neuroId_nationalIdentificationNumber_interactionTime,
    json_tmp.data.nationalIdentificationNumber.manipulation AS neuroId_nationalIdentificationNumber_manipulation,
    json_tmp.data.nationalIdentificationNumber.repeatInteractions AS neuroId_nationalIdentificationNumber_repeatInteractions,
    json_tmp.data.nationalIdentificationNumber.timeToFirstAnswer AS neuroId_nationalIdentificationNumber_timeToFirstAnswer,
    json_tmp.data.nationality.autofill AS neuroId_nationality_autofill,
    json_tmp.data.nationality.dataImport AS neuroId_nationality_dataImport,
    json_tmp.data.nationality.frictionIndex AS neuroId_nationality_frictionIndex,
    json_tmp.data.nationality.hesitation AS neuroId_nationality_hesitation,
    json_tmp.data.nationality.interactionTime AS neuroId_nationality_interactionTime,
    json_tmp.data.nationality.manipulation AS neuroId_nationality_manipulation,
    json_tmp.data.nationality.repeatInteractions AS neuroId_nationality_repeatInteractions,
    json_tmp.data.nationality.timeToFirstAnswer AS neuroId_nationality_timeToFirstAnswer,
    json_tmp.data.otpInput0.autofill AS neuroId_otpInput0_autofill,
    json_tmp.data.otpInput0.dataImport AS neuroId_otpInput0_dataImport,
    json_tmp.data.otpInput0.frictionIndex AS neuroId_otpInput0_frictionIndex,
    json_tmp.data.otpInput0.hesitation AS neuroId_otpInput0_hesitation,
    json_tmp.data.otpInput0.interactionTime AS neuroId_otpInput0_interactionTime,
    json_tmp.data.otpInput0.manipulation AS neuroId_otpInput0_manipulation,
    json_tmp.data.otpInput0.repeatInteractions AS neuroId_otpInput0_repeatInteractions,
    json_tmp.data.otpInput0.timeToFirstAnswer AS neuroId_otpInput0_timeToFirstAnswer,
    json_tmp.data.otpInput1.autofill AS neuroId_otpInput1_autofill,
    json_tmp.data.otpInput1.dataImport AS neuroId_otpInput1_dataImport,
    json_tmp.data.otpInput1.frictionIndex AS neuroId_otpInput1_frictionIndex,
    json_tmp.data.otpInput1.hesitation AS neuroId_otpInput1_hesitation,
    json_tmp.data.otpInput1.interactionTime AS neuroId_otpInput1_interactionTime,
    json_tmp.data.otpInput1.manipulation AS neuroId_otpInput1_manipulation,
    json_tmp.data.otpInput1.repeatInteractions AS neuroId_otpInput1_repeatInteractions,
    json_tmp.data.otpInput1.timeToFirstAnswer AS neuroId_otpInput1_timeToFirstAnswer,
    json_tmp.data.otpInput2.autofill AS neuroId_otpInput2_autofill,
    json_tmp.data.otpInput2.dataImport AS neuroId_otpInput2_dataImport,
    json_tmp.data.otpInput2.frictionIndex AS neuroId_otpInput2_frictionIndex,
    json_tmp.data.otpInput2.hesitation AS neuroId_otpInput2_hesitation,
    json_tmp.data.otpInput2.interactionTime AS neuroId_otpInput2_interactionTime,
    json_tmp.data.otpInput2.manipulation AS neuroId_otpInput2_manipulation,
    json_tmp.data.otpInput2.repeatInteractions AS neuroId_otpInput2_repeatInteractions,
    json_tmp.data.otpInput2.timeToFirstAnswer AS neuroId_otpInput2_timeToFirstAnswer,
    json_tmp.data.otpInput3.autofill AS neuroId_otpInput3_autofill,
    json_tmp.data.otpInput3.dataImport AS neuroId_otpInput3_dataImport,
    json_tmp.data.otpInput3.frictionIndex AS neuroId_otpInput3_frictionIndex,
    json_tmp.data.otpInput3.hesitation AS neuroId_otpInput3_hesitation,
    json_tmp.data.otpInput3.interactionTime AS neuroId_otpInput3_interactionTime,
    json_tmp.data.otpInput3.manipulation AS neuroId_otpInput3_manipulation,
    json_tmp.data.otpInput3.repeatInteractions AS neuroId_otpInput3_repeatInteractions,
    json_tmp.data.otpInput3.timeToFirstAnswer AS neuroId_otpInput3_timeToFirstAnswer,
    json_tmp.data.otpInput4.autofill AS neuroId_otpInput4_autofill,
    json_tmp.data.otpInput4.dataImport AS neuroId_otpInput4_dataImport,
    json_tmp.data.otpInput4.frictionIndex AS neuroId_otpInput4_frictionIndex,
    json_tmp.data.otpInput4.hesitation AS neuroId_otpInput4_hesitation,
    json_tmp.data.otpInput4.interactionTime AS neuroId_otpInput4_interactionTime,
    json_tmp.data.otpInput4.manipulation AS neuroId_otpInput4_manipulation,
    json_tmp.data.otpInput4.repeatInteractions AS neuroId_otpInput4_repeatInteractions,
    json_tmp.data.otpInput4.timeToFirstAnswer AS neuroId_otpInput4_timeToFirstAnswer,
    json_tmp.data.otpInput5.autofill AS neuroId_otpInput5_autofill,
    json_tmp.data.otpInput5.dataImport AS neuroId_otpInput5_dataImport,
    json_tmp.data.otpInput5.frictionIndex AS neuroId_otpInput5_frictionIndex,
    json_tmp.data.otpInput5.hesitation AS neuroId_otpInput5_hesitation,
    json_tmp.data.otpInput5.interactionTime AS neuroId_otpInput5_interactionTime,
    json_tmp.data.otpInput5.manipulation AS neuroId_otpInput5_manipulation,
    json_tmp.data.otpInput5.repeatInteractions AS neuroId_otpInput5_repeatInteractions,
    json_tmp.data.otpInput5.timeToFirstAnswer AS neuroId_otpInput5_timeToFirstAnswer,
    json_tmp.data.sessionBreaks AS neuroId_sessionBreaks,
    json_tmp.data.totalSessionIdleTime AS neuroId_totalSessionIdleTime,
    json_tmp.data.totalSessionInteractionTime AS neuroId_totalSessionInteractionTime,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'V2' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS neuroidobtained_v2_br_at -- To store it as a standalone column, when needed
FROM  {{source(  'raw_modeling', 'neuroidobtained_v2_br' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
