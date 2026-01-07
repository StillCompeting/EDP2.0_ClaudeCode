{{
    config(
        materialized='view',
        tags=['bronze', 'staging', 'mdm']
    )
}}

{#
    Staging: MDM Golden Member

    This model stages the MDM-mastered member golden records.
    Golden records represent the single source of truth for member identity
    after MDM match/merge and survivorship processing.
#}

with source as (

    select * from {{ source('mdm', 'golden_member') }}

),

staged as (

    select
        -- Primary Key
        golden_member_id,

        -- Survivorship-resolved demographics
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        date_of_birth,
        upper(trim(gender)) as gender_code,
        case upper(trim(gender))
            when 'M' then 'Male'
            when 'F' then 'Female'
            when 'U' then 'Unknown'
            else 'Unknown'
        end as gender,
        ssn,

        -- MDM Metadata
        match_confidence,
        merge_history,
        is_active,

        -- Timestamps
        created_at,
        updated_at,
        _loaded_at

    from source

)

select * from staged
