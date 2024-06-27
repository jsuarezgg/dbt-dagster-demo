{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

  select
    apps.client_id,
    case
      when min(
        greatest(
          idv.identitywaapproved_at,
          idv.identityphotosapproved_at,
          idv_3.identityphotosthirdpartyapproved_co_at
        )
      ) is not null then 1
      else 0
    end as idv_approved,
    min(
      least(
          idv.identitywaapproved_at,
          idv.identityphotosapproved_at,
          idv_3.identityphotosthirdpartyapproved_co_at
      )
    ) as first_idv_approval_date,
    max(
      greatest(
          idv.identitywaapproved_at,
          idv.identityphotosapproved_at,
          idv_3.identityphotosthirdpartyapproved_co_at
      )
    ) as last_idv_approval_date
  from
    {{ ref('f_applications_co') }} apps
    left join {{ ref('f_idv_stage_co') }} idv on apps.application_id = idv.application_id
    left join {{ ref('f_identity_photos_third_party_co') }} idv_3 on apps.application_id = idv_3.application_id
  group by
    1
