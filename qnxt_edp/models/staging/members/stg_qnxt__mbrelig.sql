with source as (

    select * from {{ source('qnxt', 'mbrelig') }}

),

staged as (

    select
        -- Keys
        {{ format_member_id('memid') }} as member_id,
        {{ clean_qnxt_string('eligid') }} as eligibility_id,

        -- Eligibility Dates
        {{ parse_qnxt_date('effdate') }} as effective_date,
        {{ parse_qnxt_date('termdate') }} as termination_date,

        -- Plan/Product Info
        {{ clean_qnxt_string('lobid') }} as lob_id,
        {{ derive_line_of_business('lobid') }} as line_of_business,
        {{ clean_qnxt_string('planid') }} as plan_id,
        {{ clean_qnxt_string('planname') }} as plan_name,
        {{ clean_qnxt_string('productid') }} as product_id,
        {{ clean_qnxt_string('groupid') }} as group_id,
        {{ clean_qnxt_string('subgroupid') }} as subgroup_id,

        -- Coverage Details
        {{ clean_qnxt_string('covtype') }} as coverage_type,
        {{ clean_qnxt_string('relation') }} as relationship_code,
        {{ clean_qnxt_string('subscriberid') }} as subscriber_id,

        -- PCP Assignment
        {{ format_provider_id('pcpid') }} as pcp_provider_id,
        {{ parse_qnxt_date('pcpassigndate') }} as pcp_assignment_date,

        -- Status
        upper(trim(eligstatus)) as eligibility_status_code,

        -- Metadata
        {{ parse_qnxt_datetime('createdate') }} as created_at,
        {{ parse_qnxt_datetime('modifydate') }} as updated_at,
        _loaded_at

    from source

)

select * from staged
