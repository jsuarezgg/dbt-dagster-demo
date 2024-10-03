{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with pii_events_bronze_co as (
    select client_id,
           id_number,
           processed_at,
           row_number() over (partition by client_id order by processed_at desc) as rn
    from {{ source('bronze', 'pii_cases_co') }}
    qualify rn = 1
),
mkt_ops_pii_cases_co as (
    select TRIM(REPLACE(REPLACE(REPLACE(client_id, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')) AS client_id,
           TRIM(REPLACE(REPLACE(REPLACE(id_number, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')) AS id_number,
        TO_DATE(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        Start_date,
                        '^([0-9]{1})/', '0\\1/'),                  -- Day adjustment
                    '/([0-9]{1})/', '/0\\1/'                       -- Month adjustment
                ),
                '/([0-9]{2})$', '/20\\1'                          -- Year adjustment
            ),
            'dd/MM/yyyy'
        ) as start_date,
           Tipo_de_exclusion___datos,
           Canal_de_exclusion_
    from {{ source('raw_modeling', 'ops_general_exclusion') }}
),
mkt_ops_pii_cases_co_deduplicated as (
    select *,
           row_number() over (partition by client_id  order by id_number desc, start_date desc) as rn
    from mkt_ops_pii_cases_co a
    qualify rn = 1
),
current_clients as (
    select distinct ls.client_id
    from {{ source('silver_live', 'f_fincore_loans_co') }} ls
    inner join {{ source('silver_live', 'f_kyc_bureau_personal_info_co') }} kyc
    on ls.client_id = kyc.client_id
    where ls.state = "CURRENT" OR ls.state = "OVERDUE"
),
final_table as (
select distinct
       coalesce(events.client_id,
                mkt_ops.client_id,
                ppd.client_id) as client_id,
       coalesce(events.id_number,
                mkt_ops.Id_Number) as id_number,
       case when events.client_id is not null then 'complete_exclusion'
            when mkt_ops.Tipo_de_exclusion___datos == 'Parcial' then 'partial_exclusion'
            else 'complete_exclusion' end as exclusion_type,
       case when cc.client_id is not null then true
            else false end as has_active_loans,
       case when (events.client_id is not null or events.id_number is not null) and
                 (mkt_ops.Client_ID is not null or mkt_ops.Id_Number is not null)
            then 'both_sources'
            when (events.client_id is not null or events.id_number is not null)
            then 'events'
            when (mkt_ops.Client_ID is not null or mkt_ops.Id_Number is not null)
            then 'ops_mkt'
            else null
        end as case_source,
       case when events.client_id is not null then 'Completa'
            when mkt_ops.Canal_de_exclusion_ == 'Completa' then 'Completa'
            else mkt_ops.Canal_de_exclusion_ end as exclusion_channel,
       coalesce(events.processed_at::date,
                mkt_ops.start_date) as exclusion_date,
       NOW() as ingested_at
from pii_events_bronze_co events
full outer join mkt_ops_pii_cases_co_deduplicated mkt_ops
on events.client_id = mkt_ops.Client_ID or events.id_number = mkt_ops.Id_Number
left join current_clients cc
on coalesce(events.client_id, mkt_ops.Client_ID) = cc.client_id
--in order to complete client_id data
left join {{ source('silver_live', 'd_prospect_personal_data_co') }} ppd
on coalesce(events.client_id, mkt_ops.client_id) is null
and coalesce(events.id_number, mkt_ops.id_number) = ppd.id_number
)
select *
from final_table;
