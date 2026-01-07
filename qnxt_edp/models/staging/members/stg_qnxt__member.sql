with source as (

    select * from {{ source('qnxt', 'member') }}

),

staged as (

    select
        -- Primary Key
        {{ format_member_id('memid') }} as member_id,

        -- Demographics
        {{ clean_qnxt_string('fname') }} as first_name,
        {{ clean_qnxt_string('mname') }} as middle_name,
        {{ clean_qnxt_string('lname') }} as last_name,
        {{ parse_qnxt_date('dob') }} as date_of_birth,
        upper(trim(sex)) as gender_code,
        {{ derive_gender('sex') }} as gender,

        -- Identifiers
        {{ clean_qnxt_string('ssn') }} as ssn,
        {{ clean_qnxt_string('medid') }} as medicaid_id,
        {{ clean_qnxt_string('mcrid') }} as medicare_id,
        {{ clean_qnxt_string('altid') }} as alternate_id,

        -- Contact Info
        {{ clean_qnxt_string('email') }} as email,
        {{ clean_qnxt_string('language') }} as preferred_language,

        -- Status
        upper(trim(status)) as status_code,
        {{ derive_member_status('status') }} as member_status,

        -- Metadata
        {{ parse_qnxt_datetime('createdate') }} as created_at,
        {{ parse_qnxt_datetime('modifydate') }} as updated_at,
        _loaded_at

    from source

)

select * from staged
