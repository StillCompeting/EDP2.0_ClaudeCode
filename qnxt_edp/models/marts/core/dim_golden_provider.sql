{{
    config(
        materialized='table',
        tags=['silver', 'core', 'dimension', 'mdm']
    )
}}

{#
    Golden Provider Dimension

    Combines MDM golden provider records with crosswalk mappings to create
    a unified provider dimension that can be joined from any source system.

    This dimension enables:
    - Stable provider identity across source system ID changes
    - Multi-source provider resolution (QNXT, CAQH, NPPES, etc.)
    - Enrichment with source-specific attributes
#}

with golden_providers as (

    select * from {{ ref('stg_mdm__golden_provider') }}
    where is_active = true

),

provider_crosswalk as (

    select * from {{ ref('stg_mdm__provider_crosswalk') }}
    where is_active = true

),

-- Get QNXT-specific enrichments for golden providers
qnxt_providers as (

    select
        provider_id,
        provider_name as qnxt_provider_name,
        npi as qnxt_npi,
        tax_id as qnxt_tax_id,
        provider_type_code as qnxt_provider_type_code,
        provider_type as qnxt_provider_type,
        provider_status_code,
        taxonomy_code as qnxt_taxonomy_code,
        case when provider_status_code = 'A' then true else false end as is_qnxt_active
    from {{ ref('stg_qnxt__provider') }}

),

-- Map QNXT provider IDs to golden IDs
qnxt_crosswalk as (

    select
        golden_provider_id,
        source_provider_id as qnxt_provider_id,
        is_primary as is_qnxt_primary
    from provider_crosswalk
    where source_system = 'QNXT'

),

-- Aggregate all source system IDs for each golden provider
source_id_summary as (

    select
        golden_provider_id,
        count(distinct source_system) as source_system_count,
        listagg(distinct source_system, ', ') within group (order by source_system) as source_systems,
        count(*) as total_crosswalk_links
    from provider_crosswalk
    group by golden_provider_id

),

final as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['gp.golden_provider_id']) }} as golden_provider_key,

        -- Golden Record Key
        gp.golden_provider_id,

        -- Primary QNXT Reference (for backwards compatibility)
        qx.qnxt_provider_id,
        qx.is_qnxt_primary,

        -- Golden Provider Data (survivorship-resolved)
        gp.provider_name,
        gp.npi,
        gp.tax_id,
        gp.provider_type_code,
        gp.primary_specialty_code,

        -- QNXT Enrichments (source-specific)
        qp.qnxt_provider_name,
        qp.qnxt_provider_type_code,
        qp.qnxt_provider_type,
        qp.provider_status_code as qnxt_status_code,
        qp.qnxt_taxonomy_code,
        qp.is_qnxt_active,

        -- MDM Quality Metrics
        gp.match_confidence,
        ss.source_system_count,
        ss.source_systems,
        ss.total_crosswalk_links,

        -- Flags
        gp.is_active as is_golden_active,
        case when qp.provider_id is not null then true else false end as has_qnxt_record,

        -- Timestamps
        gp.created_at as golden_created_at,
        gp.updated_at as golden_updated_at,
        current_timestamp() as dbt_updated_at

    from golden_providers gp
    left join qnxt_crosswalk qx on gp.golden_provider_id = qx.golden_provider_id
    left join qnxt_providers qp on qx.qnxt_provider_id = qp.provider_id
    left join source_id_summary ss on gp.golden_provider_id = ss.golden_provider_id

)

select * from final
