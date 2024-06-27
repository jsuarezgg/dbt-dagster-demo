

with initial_fpd_co as (
  select
    prospect_id as client_id,
    'CO' as country_code,
    d_vintage as origination_date,
    w_vintage as week,
    date_add(w_vintage, -84) as date_from,
    date_add(w_vintage, -55) as date_at,
    sum(
      case
        when dj.dpd_plus_1_month > 1
        and client_type = 'PROSPECT' then dj.UPB_plus_1_month
        else null
      end
    ) :: double / 3900 as upbco_usd_nominator,
    sum(
      case
        when dj.dpd_plus_1_month is not null
        and client_type = 'PROSPECT' then dj.amount
        else null
      end
    ) :: double / 3900 as opbco_usd_denominator
  from gold.risk_master_table_co dj
  where
    loan_originated is true
    and product = 'PAGO_CO'
    and d_vintage >= trunc(current_date(), 'month') - interval '13 month'
  group by
    1,
    2,
    3,
    4,
    5,
    6
),
final_fpd_co as (
  select
    i.country_code,
    cp.period,
    i.origination_date,
    i.week,
    i.client_id,
    sum(i.upbco_usd_nominator) as upbco_usd_nominator,
    sum(i.opbco_usd_denominator) as opbco_usd_denominator
  from
    initial_fpd_co i
    left join (
      select
        *,
        count(origination_date) over(partition by period) as period_n
      from
        (
          select
            distinct a.origination_date,
            a.week,
            a.date_from,
            a.date_at,
            b.date_from as df,
            b.date_at as da,
            case
              when a.origination_date between b.date_from
              and b.date_at then date_trunc('week', date_add(b.date_from, 84))
            end as period
          from
            initial_fpd_co a
            left join (
              select
                distinct origination_date,
                week,
                date_from,
                date_at
              from
                initial_fpd_co
            ) b on a.origination_date between b.date_from
            and b.date_at
        )
    ) cp on cp.origination_date = i.origination_date
    and cp.period_n = 30
  group by
    1,
    2,
    3,
    4,
    5
),
initial_fpd_br as (
  select
    prospect_id as client_id,
    'BR' as country_code,
    d_vintage as origination_date,
    w_vintage as week,
    date_add(w_vintage, -84) as date_from,
    date_add(w_vintage, -55) as date_at,
    sum(
      case
        when dpd_plus_1_month > 1
        and client_type = 'PROSPECT' then UPB_plus_1_month
      end
    ) / 5.59 as upbbr_usd_nominator,
    sum(
      case
        when dpd_plus_1_month is not null
        and client_type = 'PROSPECT' then general_amount
      end
    ) / 5.59 as opbbr_usd_denominator
  from gold.risk_master_table_br
  where
    loan_id is not null
    and d_vintage >= trunc(current_date(), 'month') - interval '13 month'
  group by
    1,
    2,
    3,
    4,
    5,
    6
),
final_fpd_br as (
  select
    i.country_code,
    cp.period,
    i.origination_date,
    i.week,
    i.client_id,
    sum(i.upbbr_usd_nominator) as upbbr_usd_nominator,
    sum(i.opbbr_usd_denominator) as opbbr_usd_denominator
  from
    initial_fpd_br i
    left join (
      select
        *,
        count(origination_date) over(partition by period) as period_n
      from
        (
          select
            distinct a.origination_date,
            a.week,
            a.date_from,
            a.date_at,
            b.date_from as df,
            b.date_at as da,
            case
              when a.origination_date between b.date_from
              and b.date_at then date_trunc('week', date_add(b.date_from, 84))
            end as period
          from
            initial_fpd_br a
            left join (
              select
                distinct origination_date,
                week,
                date_from,
                date_at
              from
                initial_fpd_br
            ) b on a.origination_date between b.date_from
            and b.date_at
        )
    ) cp on cp.origination_date = i.origination_date
    and cp.period_n = 30
  group by
    1,
    2,
    3,
    4,
    5
)

select
  country_code,
  origination_date,
  period :: date as period,
  week,
  client_id,
  sum(upbco_usd_nominator) as upb_usd_nominator,
  sum(opbco_usd_denominator) as opb_usd_denominator
from
  final_fpd_co
group by
  1,
  2,
  3,
  4,
  5
union all
select
  country_code,
  origination_date,
  period :: date as period,
  week,
  client_id,
  sum(upbbr_usd_nominator) as upb_usd_nominator,
  sum(opbbr_usd_denominator) as opb_usd_denominator
from
  final_fpd_br
group by
  1,
  2,
  3,
  4,
  5