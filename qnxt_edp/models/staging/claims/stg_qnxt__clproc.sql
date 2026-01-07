{{
    config(
        materialized='incremental',
        unique_key='claim_procedure_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

{#
    Claim Procedure Bridge Table

    Links claims to their associated procedure codes (ICD procedure codes).
    Primarily used for institutional claims with surgical/procedural information.
    Note: This is distinct from claim line procedure codes (CPT/HCPCS).
#}

with source as (

    select * from {{ source('qnxt_seeds', 'clproc') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

staged as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['claimid', 'procseq']) }} as claim_procedure_id,

        -- Foreign Keys
        {{ format_claim_id('claimid') }} as claim_id,

        -- Procedure Information
        coalesce(
            case
                when procseq is null or procseq::varchar = '' then null
                else procseq::integer
            end,
            0
        ) as procedure_sequence,
        case
            when coalesce(procseq::integer, 0) = 1 then true
            else false
        end as is_principal_procedure,

        -- Procedure Code
        {{ clean_qnxt_string('proccode') }} as procedure_code,
        upper(trim(proctype)) as procedure_code_type,
        case upper(trim(proctype))
            when 'ICD9' then 'ICD-9-PCS'
            when 'ICD10' then 'ICD-10-PCS'
            when '9' then 'ICD-9-PCS'
            when '10' then 'ICD-10-PCS'
            else 'Unknown'
        end as procedure_code_type_description,

        -- Procedure Date
        {{ parse_qnxt_date('procdate') }} as procedure_date,

        -- Metadata
        _loaded_at

    from source

)

select * from staged
