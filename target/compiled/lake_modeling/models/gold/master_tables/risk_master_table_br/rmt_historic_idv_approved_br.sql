

  select
    apps.client_id,
    case
      when min(
        greatest(
          idv.identitywaapproved_at,
          idv.identityphotosapproved_at,
          idv_3.idvthirdpartyapproved_br_at
        )
      ) is not null then 1
      else 0
    end as idv_approved,
    min(
      least(
          idv.identitywaapproved_at,
          idv.identityphotosapproved_at,
          idv_3.idvthirdpartyapproved_br_at
      )
    ) as first_idv_approval_date,
    max(
      greatest(
          idv.identitywaapproved_at,
          idv.identityphotosapproved_at,
          idv_3.idvthirdpartyapproved_br_at
      )
    ) as last_idv_approval_date
  from
    silver.f_applications_br apps
    left join silver.f_idv_stage_br idv on apps.application_id = idv.application_id
    left join silver.f_idv_third_party_br idv_3 on apps.application_id = idv_3.application_id
  group by
    1