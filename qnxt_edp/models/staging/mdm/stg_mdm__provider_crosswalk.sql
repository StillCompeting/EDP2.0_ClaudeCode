{{
    config(
        materialized='view',
        tags=['bronze', 'staging', 'mdm']
    )
}}

{#
    Staging: MDM Provider Crosswalk

    Maps source system provider IDs to MDM golden provider IDs.
    Enables resolution of any source provider ID to the mastered identity.

    Example lookups:
    - QNXT provider_id 'PROV001' → golden_provider_id 'GP-123456'
    - NPPES npi '1234567890' → golden_provider_id 'GP-123456'
#}

with source as (

    select * from {{ source('mdm', 'provider_crosswalk') }}

),

staged as (

    select
        -- Primary Key
        crosswalk_id,

        -- Golden Reference
        golden_provider_id,

        -- Source System Reference
        upper(trim(source_system)) as source_system,
        trim(source_provider_id) as source_provider_id,

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
