{{
    config(
        materialized='view',
        tags=['bronze', 'staging', 'mdm']
    )
}}

{#
    Staging: MDM Golden Provider

    This model stages the MDM-mastered provider golden records.
    Golden records represent the single source of truth for provider identity
    after MDM match/merge and survivorship processing.
#}

with source as (

    select * from {{ source('mdm', 'golden_provider') }}

),

staged as (

    select
        -- Primary Key
        golden_provider_id,

        -- Survivorship-resolved provider data
        trim(provider_name) as provider_name,
        trim(npi) as npi,
        trim(tax_id) as tax_id,
        upper(trim(provider_type)) as provider_type_code,
        trim(primary_specialty) as primary_specialty_code,

        -- MDM Metadata
        match_confidence,
        is_active,

        -- Timestamps
        created_at,
        updated_at,
        _loaded_at

    from source

)

select * from staged
