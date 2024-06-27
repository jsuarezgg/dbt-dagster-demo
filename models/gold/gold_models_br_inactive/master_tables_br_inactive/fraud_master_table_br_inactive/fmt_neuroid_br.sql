{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

  select 
  application_id,
  neuroId_email_autofill as nid_email_autofill,
  neuroId_email_dataImport as nid_email_dataImport,
  neuroId_email_hesitation as nid_email_hesitation,
  neuroId_email_manipulation as nid_email_manipulation,
  neuroId_email_frictionIndex as nid_email_frictionIndex,
  neuroId_email_interactionTime as nid_email_interactionTime,
  neuroId_email_timeToFirstAnswer as nid_email_timeToFirstAnswer,
  neuroId_email_repeatInteractions as nid_email_repeatInteractions,

  neuroId_fullName_autofill as nid_fullName_autofill,
  neuroId_fullName_dataImport as nid_fullName_dataImport,
  neuroId_fullName_hesitation as nid_fullName_hesitation,
  neuroId_fullName_manipulation as nid_fullName_manipulation,
  neuroId_fullName_frictionIndex as nid_fullName_frictionIndex,
  neuroId_fullName_interactionTime as nid_fullName_interactionTime,
  neuroId_fullName_timeToFirstAnswer as nid_fullName_timeToFirstAnswer,
  neuroId_fullName_repeatInteractions as nid_fullName_repeatInteractions,

  neuroId_cellphoneNumber_autofill as nid_cellphoneNumber_autofill,
  neuroId_cellphoneNumber_dataImport as nid_cellphoneNumber_dataImport,
  neuroId_cellphoneNumber_hesitation as nid_cellphoneNumber_hesitation,
  neuroId_cellphoneNumber_manipulation as nid_cellphoneNumber_manipulation,
  neuroId_cellphoneNumber_frictionIndex as nid_cellphoneNumber_frictionIndex,
  neuroId_cellphoneNumber_interactionTime as nid_cellphoneNumber_interactionTime,
  neuroId_cellphoneNumber_timeToFirstAnswer as nid_cellphoneNumber_timeToFirstAnswer,
  neuroId_cellphoneNumber_repeatInteractions as nid_cellphoneNumber_repeatInteractions,

  neuroId_nationalIdentificationNumber_autofill as nid_nationalIdentificationNumber_autofill,
  neuroId_nationalIdentificationNumber_dataImport as nid_nationalIdentificationNumber_dataImport,
  neuroId_nationalIdentificationNumber_hesitation as nid_nationalIdentificationNumber_hesitation,
  neuroId_nationalIdentificationNumber_manipulation as nid_nationalIdentificationNumber_manipulation,
  neuroId_nationalIdentificationNumber_frictionIndex as nid_nationalIdentificationNumber_frictionIndex,
  neuroId_nationalIdentificationNumber_interactionTime as nid_nationalIdentificationNumber_interactionTime,
  neuroId_nationalIdentificationNumber_timeToFirstAnswer as nid_nationalIdentificationNumber_timeToFirstAnswer,
  neuroId_nationalIdentificationNumber_repeatInteractions as nid_nationalIdentificationNumber_repeatInteractions,

  neuroId_sessionBreaks as nid_sessionBreaks,
  neuroId_totalSessionIdleTime as nid_totalSessionIdleTime,
  neuroId_totalSessionInteractionTime as nid_totalSessionInteractionTime

  from {{ ref('f_kyc_neuroid_v2_br') }}
