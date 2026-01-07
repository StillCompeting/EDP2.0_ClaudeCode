{{
    config(
        materialized='incremental',
        unique_key='claim_adjustment_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

{#
    Claim Adjustment Table

    Tracks all adjustments made to claims including payment adjustments,
    corrections, and other financial modifications.
#}

with source as (

    select * from {{ source('qnxt_seeds', 'cladjust') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

staged as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['claimid', 'adjustseq']) }} as claim_adjustment_id,

        -- Foreign Keys
        {{ format_claim_id('claimid') }} as claim_id,

        -- Adjustment Information
        coalesce(
            case
                when adjustseq is null or adjustseq::varchar = '' then null
                else adjustseq::integer
            end,
            0
        ) as adjustment_sequence,

        -- Adjustment Details
        {{ clean_qnxt_string('adjustcode') }} as adjustment_reason_code,
        {{ clean_qnxt_string('adjustdesc') }} as adjustment_reason_description,
        {{ clean_qnxt_string('adjustgroup') }} as adjustment_group_code,

        -- Financial
        {{ qnxt_amount('adjustamt') }} as adjustment_amount,

        -- Dates
        {{ parse_qnxt_date('adjustdate') }} as adjustment_date,

        -- Metadata
        _loaded_at

    from source

)

select * from staged
