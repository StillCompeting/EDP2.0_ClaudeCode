{{
    config(
        materialized='table',
        tags=['silver', 'core', 'dimension', 'reference']
    )
}}

{#
    Diagnosis Dimension

    Reference dimension for ICD diagnosis codes with hierarchy.
#}

with diagnosis_codes as (

    select * from {{ ref('stg_qnxt__diagcode') }}

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['diagnosis_code', 'diagnosis_code_type']) }} as diagnosis_key,

        -- Natural Key
        diagnosis_code,

        -- Descriptions
        diagnosis_description,
        diagnosis_short_description,

        -- Code System
        diagnosis_code_type,
        diagnosis_code_system,

        -- Hierarchy
        category_code,
        category_description,
        chapter_code,
        chapter_description,

        -- Derived: Major Diagnostic Category (first 3 chars for ICD-10)
        case
            when diagnosis_code_type = 'ICD10'
            then left(diagnosis_code, 3)
            else null
        end as mdc_code,

        -- Validity Period
        effective_date,
        termination_date,

        -- Derived: Is Currently Valid
        case
            when (effective_date is null or effective_date <= current_date())
            and (termination_date is null or termination_date >= current_date())
            then true else false
        end as is_current,

        -- Flags
        is_billable,
        is_header,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from diagnosis_codes

)

select * from final
