{{
    config(
        materialized='incremental',
        unique_key='claim_diagnosis_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

{#
    Claim Diagnosis Bridge Table

    Links claims to their associated diagnosis codes.
    Each claim can have multiple diagnoses with sequence indicating
    primary (1) through secondary diagnoses.
#}

with source as (

    select * from {{ source('qnxt_seeds', 'cldiag') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

staged as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['claimid', 'diagseq']) }} as claim_diagnosis_id,

        -- Foreign Keys
        {{ format_claim_id('claimid') }} as claim_id,

        -- Diagnosis Information
        coalesce(
            case
                when diagseq is null or diagseq::varchar = '' then null
                else diagseq::integer
            end,
            0
        ) as diagnosis_sequence,
        case
            when coalesce(diagseq::integer, 0) = 1 then true
            else false
        end as is_primary_diagnosis,

        -- Diagnosis Code
        {{ clean_qnxt_string('diagcode') }} as diagnosis_code,
        upper(trim(diagtype)) as diagnosis_code_type,
        case upper(trim(diagtype))
            when 'ICD9' then 'ICD-9-CM'
            when 'ICD10' then 'ICD-10-CM'
            when '9' then 'ICD-9-CM'
            when '10' then 'ICD-10-CM'
            else 'Unknown'
        end as diagnosis_code_type_description,

        -- Present on Admission (POA) Indicator
        {{ clean_qnxt_string('poa') }} as poa_indicator,
        case upper(trim(poa))
            when 'Y' then 'Yes - Present on Admission'
            when 'N' then 'No - Not Present on Admission'
            when 'U' then 'Unknown'
            when 'W' then 'Clinically Undetermined'
            when '1' then 'Unreported/Not Used'
            else null
        end as poa_description,

        -- Metadata
        _loaded_at

    from source

)

select * from staged
