{{
    config(
        materialized='table',
        tags=['gold', 'claims', 'mart']
    )
}}

{#
    Claims Summary Mart

    Aggregated claims metrics by various dimensions for analytics and reporting.
    Provides monthly/quarterly summaries with key performance indicators.
#}

with claims as (

    select * from {{ ref('fct_claim') }}

),

members as (

    select
        member_id,
        current_line_of_business,
        current_plan_id,
        current_group_id

    from {{ ref('dim_member') }}

),

claims_with_member as (

    select
        c.*,
        m.current_line_of_business,
        m.current_plan_id,
        m.current_group_id

    from claims c
    left join members m on c.member_id = m.member_id

),

monthly_summary as (

    select
        -- Dimensions
        date_trunc('month', service_from_date)::date as service_month,
        to_char(service_from_date, 'YYYY-MM') as year_month,
        extract(year from service_from_date)::int as service_year,
        extract(quarter from service_from_date)::int as service_quarter,

        current_line_of_business as line_of_business,
        claim_type,
        claim_status,

        -- Claim Counts
        count(distinct claim_id) as claim_count,
        count(distinct member_id) as unique_members,
        count(distinct billing_provider_id) as unique_providers,

        -- Status Breakdown
        sum(case when is_approved then 1 else 0 end) as approved_claims,
        sum(case when is_denied then 1 else 0 end) as denied_claims,
        sum(case when is_pending then 1 else 0 end) as pending_claims,

        -- Financial Metrics
        sum(billed_amount) as total_billed,
        sum(allowed_amount) as total_allowed,
        sum(paid_amount) as total_paid,
        sum(member_responsibility_amount) as total_member_responsibility,
        sum(discount_amount) as total_discount,

        -- Averages
        avg(billed_amount) as avg_billed_per_claim,
        avg(allowed_amount) as avg_allowed_per_claim,
        avg(paid_amount) as avg_paid_per_claim,

        -- Processing Metrics
        avg(days_to_process) as avg_days_to_process,
        avg(days_to_pay) as avg_days_to_pay,
        avg(line_count) as avg_lines_per_claim,

        -- Institutional Metrics
        sum(case when is_institutional then 1 else 0 end) as institutional_claim_count,
        sum(case when is_institutional then paid_amount else 0 end) as institutional_paid,
        avg(case when is_institutional then length_of_stay else null end) as avg_length_of_stay,

        -- Professional Metrics
        sum(case when is_professional then 1 else 0 end) as professional_claim_count,
        sum(case when is_professional then paid_amount else 0 end) as professional_paid

    from claims_with_member
    where service_from_date is not null
    group by 1, 2, 3, 4, 5, 6, 7

),

final as (

    select
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'service_month',
            'line_of_business',
            'claim_type',
            'claim_status'
        ]) }} as summary_key,

        -- Dimensions
        service_month,
        year_month,
        service_year,
        service_quarter,
        line_of_business,
        claim_type,
        claim_status,

        -- Counts
        claim_count,
        unique_members,
        unique_providers,
        approved_claims,
        denied_claims,
        pending_claims,

        -- Derived Rates
        round(approved_claims * 100.0 / nullif(claim_count, 0), 2) as approval_rate,
        round(denied_claims * 100.0 / nullif(claim_count, 0), 2) as denial_rate,

        -- Financials
        total_billed,
        total_allowed,
        total_paid,
        total_member_responsibility,
        total_discount,

        -- Derived: Payment Ratios
        round(total_allowed * 100.0 / nullif(total_billed, 0), 2) as allowed_to_billed_ratio,
        round(total_paid * 100.0 / nullif(total_allowed, 0), 2) as paid_to_allowed_ratio,

        -- Averages
        round(avg_billed_per_claim, 2) as avg_billed_per_claim,
        round(avg_allowed_per_claim, 2) as avg_allowed_per_claim,
        round(avg_paid_per_claim, 2) as avg_paid_per_claim,

        -- Processing
        round(avg_days_to_process, 1) as avg_days_to_process,
        round(avg_days_to_pay, 1) as avg_days_to_pay,
        round(avg_lines_per_claim, 1) as avg_lines_per_claim,

        -- Institutional
        institutional_claim_count,
        institutional_paid,
        round(avg_length_of_stay, 1) as avg_length_of_stay,

        -- Professional
        professional_claim_count,
        professional_paid,

        -- Per Member Metrics
        round(total_paid / nullif(unique_members, 0), 2) as paid_per_member,
        round(claim_count * 1.0 / nullif(unique_members, 0), 2) as claims_per_member,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from monthly_summary

)

select * from final
