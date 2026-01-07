with source as (

    select * from {{ source('qnxt', 'provider') }}

),

staged as (

    select
        -- Primary Key
        {{ format_provider_id('provid') }} as provider_id,

        -- Provider Identifiers
        {{ clean_qnxt_string('npi') }} as npi,
        {{ clean_qnxt_string('taxid') }} as tax_id,
        {{ clean_qnxt_string('license') }} as license_number,
        {{ clean_qnxt_string('dea') }} as dea_number,
        {{ clean_qnxt_string('upin') }} as upin,

        -- Provider Name
        {{ clean_qnxt_string('provname') }} as provider_name,
        {{ clean_qnxt_string('provlname') }} as provider_last_name,
        {{ clean_qnxt_string('provfname') }} as provider_first_name,
        {{ clean_qnxt_string('provmname') }} as provider_middle_name,
        {{ clean_qnxt_string('suffix') }} as suffix,
        {{ clean_qnxt_string('credential') }} as credential,

        -- Provider Type & Classification
        {{ clean_qnxt_string('provtype') }} as provider_type_code,
        {{ clean_qnxt_string('provtypedesc') }} as provider_type,
        {{ clean_qnxt_string('entitytype') }} as entity_type_code,
        case upper(trim(entitytype))
            when 'I' then 'Individual'
            when 'O' then 'Organization'
            else 'Unknown'
        end as entity_type,
        {{ clean_qnxt_string('taxonomy') }} as taxonomy_code,

        -- Status
        upper(trim(provstatus)) as provider_status_code,
        case upper(trim(provstatus))
            when 'A' then 'Active'
            when 'T' then 'Terminated'
            when 'P' then 'Pending'
            when 'S' then 'Suspended'
            else 'Unknown'
        end as provider_status,
        {{ parse_qnxt_date('effdate') }} as effective_date,
        {{ parse_qnxt_date('termdate') }} as termination_date,

        -- NPI Validation
        {{ is_valid_npi('npi') }} as is_valid_npi,

        -- Metadata
        {{ parse_qnxt_datetime('createdate') }} as created_at,
        {{ parse_qnxt_datetime('modifydate') }} as updated_at,
        _loaded_at

    from source

)

select * from staged
