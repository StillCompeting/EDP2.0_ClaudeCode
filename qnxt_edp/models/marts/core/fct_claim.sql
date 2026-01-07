{{
    config(
        materialized='table',
        tags=['silver', 'core', 'fact']
    )
}}

{#
    Claims Fact Table

    Central fact table containing claim header information with
    foreign keys to dimension tables.
#}

with claims as (

    select * from {{ ref('stg_qnxt__clclaim') }}

),

claim_lines as (

    select
        claim_id,
        count(*) as line_count,
        sum(billed_amount) as total_line_billed,
        sum(allowed_amount) as total_line_allowed,
        sum(paid_amount) as total_line_paid,
        sum(service_units) as total_service_units

    from {{ ref('stg_qnxt__clline') }}
    group by claim_id

),

members as (

    select member_id, member_key
    from {{ ref('dim_member') }}

),

providers as (

    select provider_id, provider_key
    from {{ ref('dim_provider') }}

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['c.claim_id']) }} as claim_key,

        -- Natural Key
        c.claim_id,

        -- Foreign Keys (Surrogate)
        m.member_key,
        bp.provider_key as billing_provider_key,
        sp.provider_key as servicing_provider_key,
        rp.provider_key as referring_provider_key,

        -- Foreign Keys (Natural)
        c.member_id,
        c.billing_provider_id,
        c.servicing_provider_id,
        c.referring_provider_id,
        c.facility_id,

        -- Date Foreign Keys
        c.service_from_date,
        c.service_to_date,
        c.admit_date,
        c.discharge_date,
        c.received_date,
        c.processed_date,
        c.paid_date,
        c.adjudicated_date,

        -- Claim Classification
        c.claim_type_code,
        c.claim_type,
        c.form_type,
        c.bill_type,

        -- Status
        c.claim_status_code,
        c.claim_status,

        -- Header Financial Amounts
        c.billed_amount,
        c.allowed_amount,
        c.paid_amount,
        c.copay_amount,
        c.coinsurance_amount,
        c.deductible_amount,
        c.cob_amount,
        c.withhold_amount,

        -- Derived: Member Responsibility
        c.copay_amount + c.coinsurance_amount + c.deductible_amount as member_responsibility_amount,

        -- Derived: Net Payment
        c.paid_amount - c.withhold_amount as net_paid_amount,

        -- Derived: Discount
        c.billed_amount - c.allowed_amount as discount_amount,

        -- Institutional Details
        c.drg_code,
        c.admit_type,
        c.admit_source,
        c.discharge_status,
        c.length_of_stay,

        -- Reference Numbers
        c.authorization_id,
        c.reference_number,
        c.internal_control_number,
        c.document_control_number,

        -- Line Aggregations
        coalesce(cl.line_count, 0) as line_count,
        coalesce(cl.total_service_units, 0) as total_service_units,

        -- Derived Flags
        case when c.claim_status_code = 'A' then true else false end as is_approved,
        case when c.claim_status_code = 'D' then true else false end as is_denied,
        case when c.claim_status_code = 'P' then true else false end as is_pending,
        case when c.paid_amount > 0 then true else false end as is_paid,
        case when c.claim_type_code = 'I' then true else false end as is_institutional,
        case when c.claim_type_code = 'P' then true else false end as is_professional,

        -- Derived: Days to Process
        datediff('day', c.received_date, c.processed_date) as days_to_process,
        datediff('day', c.processed_date, c.paid_date) as days_to_pay,
        datediff('day', c.received_date, c.paid_date) as days_to_final,

        -- Derived: Service Duration
        datediff('day', c.service_from_date, c.service_to_date) + 1 as service_days,

        -- Metadata
        c.created_at,
        c.updated_at,
        current_timestamp() as dbt_updated_at

    from claims c
    left join claim_lines cl on c.claim_id = cl.claim_id
    left join members m on c.member_id = m.member_id
    left join providers bp on c.billing_provider_id = bp.provider_id
    left join providers sp on c.servicing_provider_id = sp.provider_id
    left join providers rp on c.referring_provider_id = rp.provider_id

)

select * from final
