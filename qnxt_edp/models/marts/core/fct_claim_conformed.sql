{{
    config(
        materialized='table',
        tags=['silver', 'core', 'fact', 'mdm']
    )
}}

{#
    Conformed Claims Fact Table

    This fact table uses MDM golden IDs instead of source system IDs,
    enabling consistent member and provider identity across all analytics.

    Key benefits:
    - Member/provider identity remains stable even when source IDs change
    - Enables joining claims to data from other sources (labs, CRM, etc.)
    - Supports multi-source analytics without identity fragmentation
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

-- Crosswalks for identity resolution
member_crosswalk as (

    select
        source_member_id,
        golden_member_id
    from {{ ref('stg_mdm__member_crosswalk') }}
    where source_system = 'QNXT'
      and is_active = true

),

provider_crosswalk as (

    select
        source_provider_id,
        golden_provider_id
    from {{ ref('stg_mdm__provider_crosswalk') }}
    where source_system = 'QNXT'
      and is_active = true

),

-- Golden dimension keys
golden_members as (

    select golden_member_id, golden_member_key
    from {{ ref('dim_golden_member') }}

),

golden_providers as (

    select golden_provider_id, golden_provider_key
    from {{ ref('dim_golden_provider') }}

),

-- Legacy dimension keys (for backwards compatibility)
legacy_members as (

    select member_id, member_key
    from {{ ref('dim_member') }}

),

legacy_providers as (

    select provider_id, provider_key
    from {{ ref('dim_provider') }}

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['c.claim_id']) }} as claim_key,

        -- Natural Key
        c.claim_id,

        -- ============================================
        -- GOLDEN IDENTITY KEYS (MDM-resolved)
        -- ============================================
        
        -- Member Golden ID
        mxw.golden_member_id as member_golden_id,
        gm.golden_member_key,

        -- Provider Golden IDs
        bpxw.golden_provider_id as billing_provider_golden_id,
        gbp.golden_provider_key as billing_provider_golden_key,
        
        spxw.golden_provider_id as servicing_provider_golden_id,
        gsp.golden_provider_key as servicing_provider_golden_key,
        
        rpxw.golden_provider_id as referring_provider_golden_id,
        grp.golden_provider_key as referring_provider_golden_key,

        -- ============================================
        -- LEGACY SOURCE KEYS (for backwards compatibility)
        -- ============================================
        
        -- Legacy Member
        c.member_id as source_member_id,
        lm.member_key as legacy_member_key,

        -- Legacy Providers
        c.billing_provider_id as source_billing_provider_id,
        lbp.provider_key as legacy_billing_provider_key,
        
        c.servicing_provider_id as source_servicing_provider_id,
        lsp.provider_key as legacy_servicing_provider_key,
        
        c.referring_provider_id as source_referring_provider_id,
        lrp.provider_key as legacy_referring_provider_key,
        
        c.facility_id,

        -- ============================================
        -- CLAIM DATES
        -- ============================================
        c.service_from_date,
        c.service_to_date,
        c.admit_date,
        c.discharge_date,
        c.received_date,
        c.processed_date,
        c.paid_date,
        c.adjudicated_date,

        -- ============================================
        -- CLAIM CLASSIFICATION
        -- ============================================
        c.claim_type_code,
        c.claim_type,
        c.form_type,
        c.bill_type,
        c.claim_status_code,
        c.claim_status,

        -- ============================================
        -- FINANCIAL AMOUNTS
        -- ============================================
        c.billed_amount,
        c.allowed_amount,
        c.paid_amount,
        c.copay_amount,
        c.coinsurance_amount,
        c.deductible_amount,
        c.cob_amount,
        c.withhold_amount,

        -- Derived Amounts
        c.copay_amount + c.coinsurance_amount + c.deductible_amount as member_responsibility_amount,
        c.paid_amount - c.withhold_amount as net_paid_amount,
        c.billed_amount - c.allowed_amount as discount_amount,

        -- ============================================
        -- INSTITUTIONAL DETAILS
        -- ============================================
        c.drg_code,
        c.admit_type,
        c.admit_source,
        c.discharge_status,
        c.length_of_stay,

        -- ============================================
        -- REFERENCE NUMBERS
        -- ============================================
        c.authorization_id,
        c.reference_number,
        c.internal_control_number,
        c.document_control_number,

        -- ============================================
        -- LINE AGGREGATIONS
        -- ============================================
        coalesce(cl.line_count, 0) as line_count,
        coalesce(cl.total_service_units, 0) as total_service_units,

        -- ============================================
        -- DERIVED FLAGS
        -- ============================================
        case when c.claim_status_code = 'A' then true else false end as is_approved,
        case when c.claim_status_code = 'D' then true else false end as is_denied,
        case when c.claim_status_code = 'P' then true else false end as is_pending,
        case when c.paid_amount > 0 then true else false end as is_paid,
        case when c.claim_type_code = 'I' then true else false end as is_institutional,
        case when c.claim_type_code = 'P' then true else false end as is_professional,

        -- Identity Resolution Flags
        case when mxw.golden_member_id is not null then true else false end as has_golden_member,
        case when bpxw.golden_provider_id is not null then true else false end as has_golden_billing_provider,
        case when spxw.golden_provider_id is not null then true else false end as has_golden_servicing_provider,

        -- ============================================
        -- DERIVED METRICS
        -- ============================================
        datediff('day', c.received_date, c.processed_date) as days_to_process,
        datediff('day', c.processed_date, c.paid_date) as days_to_pay,
        datediff('day', c.received_date, c.paid_date) as days_to_final,
        datediff('day', c.service_from_date, c.service_to_date) + 1 as service_days,

        -- ============================================
        -- METADATA
        -- ============================================
        c.created_at,
        c.updated_at,
        current_timestamp() as dbt_updated_at

    from claims c
    
    -- Line aggregations
    left join claim_lines cl on c.claim_id = cl.claim_id
    
    -- MDM Identity Resolution: Member
    left join member_crosswalk mxw on c.member_id = mxw.source_member_id
    left join golden_members gm on mxw.golden_member_id = gm.golden_member_id
    
    -- MDM Identity Resolution: Billing Provider
    left join provider_crosswalk bpxw on c.billing_provider_id = bpxw.source_provider_id
    left join golden_providers gbp on bpxw.golden_provider_id = gbp.golden_provider_id
    
    -- MDM Identity Resolution: Servicing Provider
    left join provider_crosswalk spxw on c.servicing_provider_id = spxw.source_provider_id
    left join golden_providers gsp on spxw.golden_provider_id = gsp.golden_provider_id
    
    -- MDM Identity Resolution: Referring Provider
    left join provider_crosswalk rpxw on c.referring_provider_id = rpxw.source_provider_id
    left join golden_providers grp on rpxw.golden_provider_id = grp.golden_provider_id
    
    -- Legacy Dimension Keys (for backwards compatibility)
    left join legacy_members lm on c.member_id = lm.member_id
    left join legacy_providers lbp on c.billing_provider_id = lbp.provider_id
    left join legacy_providers lsp on c.servicing_provider_id = lsp.provider_id
    left join legacy_providers lrp on c.referring_provider_id = lrp.provider_id

)

select * from final
