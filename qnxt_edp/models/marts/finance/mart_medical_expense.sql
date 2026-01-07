{{
    config(
        materialized='table',
        tags=['gold', 'finance', 'mart']
    )
}}

{#
    Medical Expense Mart

    Financial reporting mart combining claims payments with member months
    to calculate PMPM (Per Member Per Month) metrics and medical loss ratios.
#}

with claims_summary as (

    select
        service_month,
        line_of_business,
        claim_type,

        sum(claim_count) as claim_count,
        sum(total_billed) as total_billed,
        sum(total_allowed) as total_allowed,
        sum(total_paid) as total_paid,
        sum(total_member_responsibility) as total_member_responsibility,
        sum(institutional_paid) as institutional_paid,
        sum(professional_paid) as professional_paid,
        sum(unique_members) as claims_members

    from {{ ref('mart_claims_summary') }}
    group by 1, 2, 3

),

member_months as (

    select
        calendar_month,
        line_of_business,
        sum(member_months) as member_months,
        sum(enrolled_members) as enrolled_members

    from {{ ref('mart_member_months') }}
    group by 1, 2

),

combined as (

    select
        coalesce(cs.service_month, mm.calendar_month) as report_month,
        to_char(coalesce(cs.service_month, mm.calendar_month), 'YYYY-MM') as year_month,
        extract(year from coalesce(cs.service_month, mm.calendar_month))::int as report_year,
        extract(quarter from coalesce(cs.service_month, mm.calendar_month))::int as report_quarter,

        coalesce(cs.line_of_business, mm.line_of_business) as line_of_business,
        cs.claim_type,

        -- Claims Metrics
        coalesce(cs.claim_count, 0) as claim_count,
        coalesce(cs.total_billed, 0) as total_billed,
        coalesce(cs.total_allowed, 0) as total_allowed,
        coalesce(cs.total_paid, 0) as total_paid,
        coalesce(cs.total_member_responsibility, 0) as total_member_responsibility,
        coalesce(cs.institutional_paid, 0) as institutional_paid,
        coalesce(cs.professional_paid, 0) as professional_paid,
        coalesce(cs.claims_members, 0) as members_with_claims,

        -- Member Months
        coalesce(mm.member_months, 0) as member_months,
        coalesce(mm.enrolled_members, 0) as enrolled_members

    from claims_summary cs
    full outer join member_months mm
        on cs.service_month = mm.calendar_month
        and cs.line_of_business = mm.line_of_business

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'report_month',
            'line_of_business',
            'claim_type'
        ]) }} as expense_key,

        -- Time Dimensions
        report_month,
        year_month,
        report_year,
        report_quarter,

        -- Dimensions
        line_of_business,
        claim_type,

        -- Volume Metrics
        claim_count,
        enrolled_members,
        members_with_claims,
        member_months,

        -- Financial Totals
        total_billed,
        total_allowed,
        total_paid,
        total_member_responsibility,
        institutional_paid,
        professional_paid,

        -- PMPM Metrics (Per Member Per Month)
        case
            when member_months > 0
            then round(total_paid / member_months, 2)
            else 0
        end as paid_pmpm,

        case
            when member_months > 0
            then round(total_allowed / member_months, 2)
            else 0
        end as allowed_pmpm,

        case
            when member_months > 0
            then round(total_billed / member_months, 2)
            else 0
        end as billed_pmpm,

        case
            when member_months > 0
            then round(institutional_paid / member_months, 2)
            else 0
        end as institutional_pmpm,

        case
            when member_months > 0
            then round(professional_paid / member_months, 2)
            else 0
        end as professional_pmpm,

        -- Per Claim Metrics
        case
            when claim_count > 0
            then round(total_paid / claim_count, 2)
            else 0
        end as avg_paid_per_claim,

        -- Utilization Metrics
        case
            when enrolled_members > 0
            then round(members_with_claims * 100.0 / enrolled_members, 2)
            else 0
        end as utilization_rate,

        case
            when member_months > 0
            then round(claim_count * 1000.0 / member_months, 1)
            else 0
        end as claims_per_1000_mm,

        -- Cost Distribution
        case
            when total_paid > 0
            then round(institutional_paid * 100.0 / total_paid, 2)
            else 0
        end as institutional_pct,

        case
            when total_paid > 0
            then round(professional_paid * 100.0 / total_paid, 2)
            else 0
        end as professional_pct,

        -- Discount Analysis
        total_billed - total_allowed as total_discount,
        case
            when total_billed > 0
            then round((total_billed - total_allowed) * 100.0 / total_billed, 2)
            else 0
        end as discount_pct,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from combined

)

select * from final
