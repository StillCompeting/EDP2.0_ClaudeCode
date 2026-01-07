{{
    config(
        materialized='table',
        tags=['silver', 'core', 'dimension']
    )
}}

{#
    Provider Dimension

    Comprehensive provider dimension with specialty and network information.
#}

with providers as (

    select * from {{ ref('stg_qnxt__provider') }}

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['provider_id']) }} as provider_key,

        -- Natural Key
        provider_id,

        -- Identifiers
        npi,
        tax_id,
        license_number,
        dea_number,
        upin,

        -- Provider Name
        provider_name,
        provider_last_name,
        provider_first_name,
        provider_middle_name,
        suffix,
        credential,

        -- Formatted Names
        case
            when entity_type_code = 'I' then
                coalesce(provider_last_name, '') ||
                case when provider_first_name is not null then ', ' || provider_first_name else '' end ||
                case when credential is not null then ', ' || credential else '' end
            else provider_name
        end as display_name,

        -- Classification
        provider_type_code,
        provider_type,
        entity_type_code,
        entity_type,
        taxonomy_code,

        -- Status
        provider_status_code,
        provider_status,
        effective_date,
        termination_date,

        -- Derived Flags
        case
            when provider_status_code = 'A'
            and (termination_date is null or termination_date >= current_date())
            then true else false
        end as is_active,

        is_valid_npi,

        -- Metadata
        created_at,
        updated_at,
        current_timestamp() as dbt_updated_at

    from providers

)

select * from final
