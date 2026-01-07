{{
    config(
        materialized='view',
        tags=['bronze', 'staging', 'mdm']
    )
}}

{#
    Staging: MDM Member Crosswalk

    Maps source system member IDs to MDM golden member IDs.
    Enables resolution of any source member ID to the mastered identity.

    Example lookups:
    - QNXT member_id 'MBR001' → golden_member_id 'GM-123456'
    - LAB patient_id 'PAT-789' → golden_member_id 'GM-123456'
#}

with source as (

    select * from {{ source('mdm', 'member_crosswalk') }}

),

staged as (

    select
        -- Primary Key
        crosswalk_id,

        -- Golden Reference
        golden_member_id,

        -- Source System Reference
        upper(trim(source_system)) as source_system,
        trim(source_member_id) as source_member_id,

        -- Match Quality
        match_confidence,
        trim(match_rule) as match_rule,
        coalesce(is_primary, false) as is_primary,

        -- Effectivity
        effective_date,
        termination_date,

        -- Is this crosswalk currently active?
        case
            when termination_date is null then true
            when termination_date >= current_date() then true
            else false
        end as is_active,

        -- Timestamps
        created_at,
        _loaded_at

    from source

)

select * from staged
