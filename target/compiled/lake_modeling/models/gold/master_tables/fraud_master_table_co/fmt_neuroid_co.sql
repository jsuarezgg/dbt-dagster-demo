

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

  neuroId_lastName_autofill as nid_lastName_autofill,
  neuroId_lastName_dataImport as nid_lastName_dataImport,
  neuroId_lastName_hesitation as nid_lastName_hesitation,
  neuroId_lastName_manipulation as nid_lastName_manipulation,
  neuroId_lastName_frictionIndex as nid_lastName_frictionIndex,
  neuroId_lastName_interactionTime as nid_lastName_interactionTime,
  neuroId_lastName_timeToFirstAnswer as nid_lastName_timeToFirstAnswer,
  neuroId_lastName_repeatInteractions as nid_lastName_repeatInteractions,

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

  neuroId_nationalExpeditionDateDay_autofill as nid_nationalExpeditionDateDay_autofill,
  neuroId_nationalExpeditionDateDay_dataImport as nid_nationalExpeditionDateDay_dataImport,
  neuroId_nationalExpeditionDateDay_hesitation as nid_nationalExpeditionDateDay_hesitation,
  neuroId_nationalExpeditionDateDay_manipulation as nid_nationalExpeditionDateDay_manipulation,
  neuroId_nationalExpeditionDateDay_frictionIndex as nid_nationalExpeditionDateDay_frictionIndex,
  neuroId_nationalExpeditionDateDay_interactionTime as nid_nationalExpeditionDateDay_interactionTime,
  neuroId_nationalExpeditionDateDay_timeToFirstAnswer as nid_nationalExpeditionDateDay_timeToFirstAnswer,
  neuroId_nationalExpeditionDateDay_repeatInteractions as nid_nationalExpeditionDateDay_repeatInteractions,

  neuroId_nationalExpeditionDateYear_autofill as nid_nationalExpeditionDateYear_autofill,
  neuroId_nationalExpeditionDateYear_dataImport as nid_nationalExpeditionDateYear_dataImport,
  neuroId_nationalExpeditionDateYear_hesitation as nid_nationalExpeditionDateYear_hesitation,
  neuroId_nationalExpeditionDateYear_manipulation as nid_nationalExpeditionDateYear_manipulation,
  neuroId_nationalExpeditionDateYear_frictionIndex as nid_nationalExpeditionDateYear_frictionIndex,
  neuroId_nationalExpeditionDateYear_interactionTime as nid_nationalExpeditionDateYear_interactionTime,
  neuroId_nationalExpeditionDateYear_timeToFirstAnswer as nid_nationalExpeditionDateYear_timeToFirstAnswer,
  neuroId_nationalExpeditionDateYear_repeatInteractions as nid_nationalExpeditionDateYear_repeatInteractions,

  neuroId_nationalExpeditionDateMonth_autofill as nid_nationalExpeditionDateMonth_autofill,
  neuroId_nationalExpeditionDateMonth_dataImport as nid_nationalExpeditionDateMonth_dataImport,
  neuroId_nationalExpeditionDateMonth_hesitation as nid_nationalExpeditionDateMonth_hesitation,
  neuroId_nationalExpeditionDateMonth_manipulation as nid_nationalExpeditionDateMonth_manipulation,
  neuroId_nationalExpeditionDateMonth_frictionIndex as nid_nationalExpeditionDateMonth_frictionIndex,
  neuroId_nationalExpeditionDateMonth_interactionTime as nid_nationalExpeditionDateMonth_interactionTime,
  neuroId_nationalExpeditionDateMonth_timeToFirstAnswer as nid_nationalExpeditionDateMonth_timeToFirstAnswer,
  neuroId_nationalExpeditionDateMonth_repeatInteractions as nid_nationalExpeditionDateMonth_repeatInteractions,

  neuroId_sessionBreaks as nid_sessionBreaks,
  neuroId_totalSessionIdleTime as nid_totalSessionIdleTime,
  neuroId_totalSessionInteractionTime as nid_totalSessionInteractionTime

  from silver.f_kyc_neuroid_v2_co