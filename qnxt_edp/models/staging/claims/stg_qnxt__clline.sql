{{
    config(
        materialized='incremental',
        unique_key='claim_line_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with source as (

    select * from {{ source('qnxt', 'clline') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

staged as (

    select
        -- Composite Key
        {{ format_claim_id('claimid') }} as claim_id,
        case
            when lineseq is null or lineseq::varchar = '' then null
            else lineseq::integer
        end as line_number,

        -- Generate surrogate key for the line
        {{ dbt_utils.generate_surrogate_key(['claimid', 'lineseq']) }} as claim_line_id,

        -- Procedure & Revenue Codes
        {{ clean_qnxt_string('proccode') }} as procedure_code,
        {{ clean_qnxt_string('modifier1') }} as modifier_1,
        {{ clean_qnxt_string('modifier2') }} as modifier_2,
        {{ clean_qnxt_string('modifier3') }} as modifier_3,
        {{ clean_qnxt_string('modifier4') }} as modifier_4,
        {{ clean_qnxt_string('revcode') }} as revenue_code,
        {{ clean_qnxt_string('ndc') }} as ndc_code,

        -- Service Details
        {{ parse_qnxt_date('svcfromdate') }} as service_from_date,
        {{ parse_qnxt_date('svctodate') }} as service_to_date,
        {{ clean_qnxt_string('pos') }} as place_of_service_code,
        coalesce(
            case
                when svcunits is null or svcunits::varchar = '' then null
                else svcunits::number(18,2)
            end,
            0
        ) as service_units,

        -- Financial Amounts
        {{ qnxt_amount('billedamt') }} as billed_amount,
        {{ qnxt_amount('allowedamt') }} as allowed_amount,
        {{ qnxt_amount('paidamt') }} as paid_amount,
        {{ qnxt_amount('copayamt') }} as copay_amount,
        {{ qnxt_amount('coinsamt') }} as coinsurance_amount,
        {{ qnxt_amount('deductamt') }} as deductible_amount,
        {{ qnxt_amount('cobamt') }} as cob_amount,
        {{ qnxt_amount('withholdamt') }} as withhold_amount,

        -- Status
        upper(trim(linestatus)) as line_status_code,
        {{ derive_claim_status('linestatus') }} as line_status,

        -- Diagnosis Pointers
        {{ clean_qnxt_string('diagptr1') }} as diagnosis_pointer_1,
        {{ clean_qnxt_string('diagptr2') }} as diagnosis_pointer_2,
        {{ clean_qnxt_string('diagptr3') }} as diagnosis_pointer_3,
        {{ clean_qnxt_string('diagptr4') }} as diagnosis_pointer_4,

        -- Provider
        {{ format_provider_id('servprovid') }} as servicing_provider_id,

        -- Metadata
        {{ parse_qnxt_datetime('createdate') }} as created_at,
        {{ parse_qnxt_datetime('modifydate') }} as updated_at,
        _loaded_at

    from source

)

select * from staged
