{{
    config(
        materialized='table',
        tags=['silver', 'core', 'dimension']
    )
}}

{#
    Member Dimension

    Combines member demographics with current eligibility information
    to create a comprehensive member dimension.
#}

with members as (

    select * from {{ ref('stg_qnxt__member') }}

),

eligibility as (

    select * from {{ ref('stg_qnxt__mbrelig') }}

),

-- Get most recent eligibility for each member
current_eligibility as (

    select
        member_id,
        eligibility_id,
        effective_date,
        termination_date,
        lob_id,
        line_of_business,
        plan_id,
        plan_name,
        product_id,
        group_id,
        subgroup_id,
        coverage_type,
        relationship_code,
        subscriber_id,
        pcp_provider_id,
        eligibility_status_code,
        row_number() over (
            partition by member_id
            order by effective_date desc nulls last, termination_date desc nulls last
        ) as rn

    from eligibility

),

latest_eligibility as (

    select * from current_eligibility where rn = 1

),

-- Get all active eligibility segments for member months calculation
active_eligibility as (

    select
        member_id,
        count(distinct eligibility_id) as total_eligibility_segments,
        min(effective_date) as first_effective_date,
        max(coalesce(termination_date, '9999-12-31'::date)) as last_termination_date,
        sum(
            case
                when effective_date is null then 0
                when termination_date is null then {{ calculate_member_months('effective_date', 'current_date()') }}
                else {{ calculate_member_months('effective_date', 'termination_date') }}
            end
        ) as total_member_months

    from eligibility
    group by member_id

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['m.member_id']) }} as member_key,

        -- Natural Key
        m.member_id,

        -- Demographics
        m.first_name,
        m.middle_name,
        m.last_name,
        m.first_name || ' ' || m.last_name as full_name,
        m.date_of_birth,
        {{ calculate_age('m.date_of_birth') }} as current_age,
        m.gender_code,
        m.gender,
        m.ssn,
        m.medicaid_id,
        m.medicare_id,

        -- Contact
        m.email,
        m.preferred_language,

        -- Status
        m.status_code,
        m.member_status,

        -- Current Eligibility
        le.eligibility_id as current_eligibility_id,
        le.effective_date as current_elig_effective_date,
        le.termination_date as current_elig_termination_date,
        le.lob_id as current_lob_id,
        le.line_of_business as current_line_of_business,
        le.plan_id as current_plan_id,
        le.plan_name as current_plan_name,
        le.product_id as current_product_id,
        le.group_id as current_group_id,
        le.coverage_type as current_coverage_type,
        le.relationship_code,
        le.subscriber_id,
        le.pcp_provider_id as current_pcp_provider_id,

        -- Eligibility Summary
        ae.total_eligibility_segments,
        ae.first_effective_date,
        ae.last_termination_date,
        ae.total_member_months,

        -- Derived Flags
        case
            when le.termination_date is null or le.termination_date >= current_date()
            then true else false
        end as is_currently_eligible,

        case
            when le.relationship_code = '18' or le.relationship_code = 'SELF'
            then true else false
        end as is_subscriber,

        -- Metadata
        m.created_at,
        m.updated_at,
        current_timestamp() as dbt_updated_at

    from members m
    left join latest_eligibility le on m.member_id = le.member_id
    left join active_eligibility ae on m.member_id = ae.member_id

)

select * from final
