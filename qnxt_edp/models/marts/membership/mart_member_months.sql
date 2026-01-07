{{
    config(
        materialized='table',
        tags=['gold', 'membership', 'mart']
    )
}}

{#
    Member Months Mart

    Calculates member months and enrollment metrics by various dimensions.
    Critical for PMPM calculations and population health analytics.
#}

with eligibility as (

    select * from {{ ref('stg_qnxt__mbrelig') }}

),

date_spine as (

    select date_day as calendar_month
    from {{ ref('dim_date') }}
    where day_of_month = 1
      and date_day >= dateadd('year', -5, current_date())
      and date_day <= current_date()

),

-- Expand eligibility segments to individual months
member_month_detail as (

    select
        e.member_id,
        e.eligibility_id,
        ds.calendar_month,
        e.lob_id,
        e.line_of_business,
        e.plan_id,
        e.plan_name,
        e.product_id,
        e.group_id,
        e.subgroup_id,
        e.coverage_type,
        e.relationship_code,
        e.pcp_provider_id,

        -- Derive member month fraction (for partial months)
        case
            -- Full month
            when e.effective_date <= ds.calendar_month
             and (e.termination_date is null or e.termination_date >= last_day(ds.calendar_month))
            then 1.0

            -- Partial month - started mid-month
            when e.effective_date > ds.calendar_month
             and e.effective_date <= last_day(ds.calendar_month)
             and (e.termination_date is null or e.termination_date >= last_day(ds.calendar_month))
            then (datediff('day', e.effective_date, last_day(ds.calendar_month)) + 1)::float
                 / (datediff('day', ds.calendar_month, last_day(ds.calendar_month)) + 1)

            -- Partial month - ended mid-month
            when e.effective_date <= ds.calendar_month
             and e.termination_date >= ds.calendar_month
             and e.termination_date < last_day(ds.calendar_month)
            then (datediff('day', ds.calendar_month, e.termination_date) + 1)::float
                 / (datediff('day', ds.calendar_month, last_day(ds.calendar_month)) + 1)

            -- Partial month - started and ended mid-month
            when e.effective_date > ds.calendar_month
             and e.termination_date < last_day(ds.calendar_month)
            then (datediff('day', e.effective_date, e.termination_date) + 1)::float
                 / (datediff('day', ds.calendar_month, last_day(ds.calendar_month)) + 1)

            else 0
        end as member_month_fraction

    from eligibility e
    cross join date_spine ds
    where ds.calendar_month >= e.effective_date
      and (e.termination_date is null or ds.calendar_month <= e.termination_date)

),

monthly_aggregates as (

    select
        -- Time Dimensions
        calendar_month,
        to_char(calendar_month, 'YYYY-MM') as year_month,
        extract(year from calendar_month)::int as calendar_year,
        extract(quarter from calendar_month)::int as calendar_quarter,
        extract(month from calendar_month)::int as calendar_month_num,

        -- Grouping Dimensions
        line_of_business,
        plan_id,
        plan_name,
        group_id,
        coverage_type,

        -- Member Months (full equivalents)
        sum(member_month_fraction) as member_months,

        -- Counts
        count(distinct member_id) as enrolled_members,
        count(distinct case when member_month_fraction = 1 then member_id end) as full_month_members,
        count(distinct eligibility_id) as active_eligibility_segments,
        count(distinct pcp_provider_id) as assigned_pcps,

        -- Relationship Breakdown
        count(distinct case when relationship_code in ('18', 'SELF') then member_id end) as subscriber_count,
        count(distinct case when relationship_code not in ('18', 'SELF') then member_id end) as dependent_count

    from member_month_detail
    where member_month_fraction > 0
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'calendar_month',
            'line_of_business',
            'plan_id',
            'group_id',
            'coverage_type'
        ]) }} as member_month_key,

        -- Time
        calendar_month,
        year_month,
        calendar_year,
        calendar_quarter,
        calendar_month_num,

        -- Dimensions
        line_of_business,
        plan_id,
        plan_name,
        group_id,
        coverage_type,

        -- Member Months
        round(member_months, 2) as member_months,

        -- Counts
        enrolled_members,
        full_month_members,
        active_eligibility_segments,
        assigned_pcps,
        subscriber_count,
        dependent_count,

        -- Derived Ratios
        round(dependent_count * 1.0 / nullif(subscriber_count, 0), 2) as dependent_ratio,
        round(enrolled_members * 1.0 / nullif(assigned_pcps, 0), 1) as members_per_pcp,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from monthly_aggregates

)

select * from final
