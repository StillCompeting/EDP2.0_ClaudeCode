{{
    config(
        materialized='incremental',
        unique_key='claim_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with source as (

    select * from {{ source('qnxt', 'clclaim') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

staged as (

    select
        -- Primary Key
        {{ format_claim_id('claimid') }} as claim_id,

        -- Foreign Keys
        {{ format_member_id('memid') }} as member_id,
        {{ format_provider_id('provid') }} as billing_provider_id,
        {{ format_provider_id('servprovid') }} as servicing_provider_id,
        {{ format_provider_id('refprovid') }} as referring_provider_id,
        {{ clean_qnxt_string('facilityid') }} as facility_id,

        -- Claim Type & Classification
        upper(trim(claimtype)) as claim_type_code,
        case upper(trim(claimtype))
            when 'I' then 'Institutional'
            when 'P' then 'Professional'
            when 'D' then 'Dental'
            when 'R' then 'Pharmacy'
            else 'Unknown'
        end as claim_type,
        {{ clean_qnxt_string('formtype') }} as form_type,
        {{ clean_qnxt_string('billtype') }} as bill_type,

        -- Service Dates
        {{ parse_qnxt_date('svcfromdate') }} as service_from_date,
        {{ parse_qnxt_date('svctodate') }} as service_to_date,
        {{ parse_qnxt_date('admitdate') }} as admit_date,
        {{ parse_qnxt_date('dischargedate') }} as discharge_date,

        -- Processing Dates
        {{ parse_qnxt_date('recvddate') }} as received_date,
        {{ parse_qnxt_date('processdate') }} as processed_date,
        {{ parse_qnxt_date('paiddate') }} as paid_date,
        {{ parse_qnxt_date('adjuddate') }} as adjudicated_date,

        -- Status
        upper(trim(claimstatus)) as claim_status_code,
        {{ derive_claim_status('claimstatus') }} as claim_status,

        -- Financial Amounts
        {{ qnxt_amount('billedamt') }} as billed_amount,
        {{ qnxt_amount('allowedamt') }} as allowed_amount,
        {{ qnxt_amount('paidamt') }} as paid_amount,
        {{ qnxt_amount('copayamt') }} as copay_amount,
        {{ qnxt_amount('coinsamt') }} as coinsurance_amount,
        {{ qnxt_amount('deductamt') }} as deductible_amount,
        {{ qnxt_amount('cobamt') }} as cob_amount,
        {{ qnxt_amount('withholdamt') }} as withhold_amount,

        -- Institutional Specific
        {{ clean_qnxt_string('drg') }} as drg_code,
        {{ clean_qnxt_string('admittype') }} as admit_type,
        {{ clean_qnxt_string('admitsource') }} as admit_source,
        {{ clean_qnxt_string('dischargestatus') }} as discharge_status,
        coalesce(
            case
                when los is null or los::varchar = '' then null
                else los::integer
            end,
            0
        ) as length_of_stay,

        -- Reference Numbers
        {{ clean_qnxt_string('authid') }} as authorization_id,
        {{ clean_qnxt_string('refnum') }} as reference_number,
        {{ clean_qnxt_string('icn') }} as internal_control_number,
        {{ clean_qnxt_string('dcn') }} as document_control_number,

        -- Metadata
        {{ parse_qnxt_datetime('createdate') }} as created_at,
        {{ parse_qnxt_datetime('modifydate') }} as updated_at,
        _loaded_at

    from source

)

select * from staged
