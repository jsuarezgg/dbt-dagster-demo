{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT DISTINCT
    surveyId AS survey_id,
    surveyName AS survey_name,
    visitorId AS visitor_id,
    URL AS url,
    CreatedAt AS survey_created_at,
    CompletedAt AS survey_completed_at,
    UserID AS user_id,
    OS AS os,
    Browser AS browser,
    UserAgent AS user_agent,
    CustomMetadata AS custom_metadata,
    TriggeringEvent AS triggering_event,
    ResponseGroupID AS response_group_id,
    QuestionNumber AS question_number,
    Question AS question,
    Response AS response,
    Themes AS themes,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('bronze', 'sprig_survey_ally_portal') }}
WHERE surveyName not like '%NPS%' or QuestionNumber <> 'Q1'
or (surveyName like '%NPS%' and QuestionNumber = 'Q1' and response not in ('detractor','passive','promoter'))
