with source as (

    select * from {{ source('qnxt', 'diagcode') }}

),

staged as (

    select
        -- Primary Key
        {{ clean_qnxt_string('diagcode') }} as diagnosis_code,

        -- Description
        {{ clean_qnxt_string('diagdesc') }} as diagnosis_description,
        {{ clean_qnxt_string('diagshortdesc') }} as diagnosis_short_description,

        -- Classification
        upper(trim(diagtype)) as diagnosis_code_type,
        case upper(trim(diagtype))
            when 'ICD9' then 'ICD-9-CM'
            when 'ICD10' then 'ICD-10-CM'
            else upper(trim(diagtype))
        end as diagnosis_code_system,

        -- Hierarchy
        {{ clean_qnxt_string('category') }} as category_code,
        {{ clean_qnxt_string('categorydesc') }} as category_description,
        {{ clean_qnxt_string('chapter') }} as chapter_code,
        {{ clean_qnxt_string('chapterdesc') }} as chapter_description,

        -- Validity Period
        {{ parse_qnxt_date('effdate') }} as effective_date,
        {{ parse_qnxt_date('termdate') }} as termination_date,

        -- Flags
        coalesce(upper(trim(billable)) = 'Y', false) as is_billable,
        coalesce(upper(trim(header)) = 'Y', false) as is_header,

        -- Metadata
        _loaded_at

    from source

)

select * from staged
