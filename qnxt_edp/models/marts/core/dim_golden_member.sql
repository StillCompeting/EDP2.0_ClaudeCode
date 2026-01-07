{{
    config(
        materialized='table',
        tags=['silver', 'core', 'dimension', 'mdm']
    )
}}

{#
    Golden Member Dimension

    Combines MDM golden member records with crosswalk mappings to create
    a unified member dimension that can be joined from any source system.

    This dimension enables:
    - Stable member identity across source system ID changes
    - Multi-source member resolution (QNXT, Labs, CRM, etc.)
    - Enrichment with source-specific attributes
#}

with golden_members as (

    select * from {{ ref('stg_mdm__golden_member') }}
    where is_active = true

),

member_crosswalk as (

    select * from {{ ref('stg_mdm__member_crosswalk') }}
    where is_active = true

),

-- Get QNXT-specific enrichments for golden members
qnxt_members as (

    select
        member_id,
        medicaid_id,
        medicare_id,
        email,
        preferred_language,
        status_code,
        member_status
    from {{ ref('stg_qnxt__member') }}

),

-- Map QNXT member IDs to golden IDs
qnxt_crosswalk as (

    select
        golden_member_id,
        source_member_id as qnxt_member_id,
        is_primary as is_qnxt_primary
    from member_crosswalk
    where source_system = 'QNXT'

),

-- Aggregate all source system IDs for each golden member
source_id_summary as (

    select
        golden_member_id,
        count(distinct source_system) as source_system_count,
        listagg(distinct source_system, ', ') within group (order by source_system) as source_systems,
        count(*) as total_crosswalk_links
    from member_crosswalk
    group by golden_member_id

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['gm.golden_member_id']) }} as golden_member_key,

        -- Golden Record Key
        gm.golden_member_id,

        -- Primary QNXT Reference (for backwards compatibility)
        qx.qnxt_member_id,
        qx.is_qnxt_primary,

        -- Golden Demographics (survivorship-resolved)
        gm.first_name,
        gm.last_name,
        gm.first_name || ' ' || gm.last_name as full_name,
        gm.date_of_birth,
        {{ calculate_age('gm.date_of_birth') }} as current_age,
        gm.gender_code,
        gm.gender,
        gm.ssn,

        -- QNXT Enrichments (source-specific)
        qm.medicaid_id,
        qm.medicare_id,
        qm.email,
        qm.preferred_language,
        qm.status_code as qnxt_status_code,
        qm.member_status as qnxt_member_status,

        -- MDM Quality Metrics
        gm.match_confidence,
        ss.source_system_count,
        ss.source_systems,
        ss.total_crosswalk_links,

        -- Flags
        gm.is_active as is_golden_active,
        case when qm.member_id is not null then true else false end as has_qnxt_record,

        -- Timestamps
        gm.created_at as golden_created_at,
        gm.updated_at as golden_updated_at,
        current_timestamp() as dbt_updated_at

    from golden_members gm
    left join qnxt_crosswalk qx on gm.golden_member_id = qx.golden_member_id
    left join qnxt_members qm on qx.qnxt_member_id = qm.member_id
    left join source_id_summary ss on gm.golden_member_id = ss.golden_member_id

)

select * from final
