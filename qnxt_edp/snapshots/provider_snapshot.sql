{% snapshot provider_snapshot %}
{#
    Provider Snapshot (SCD Type 2)

    Tracks historical changes to provider information including
    status changes, credential updates, and network affiliations.
    Uses timestamp strategy based on the updated_at column.
#}

{{
    config(
        target_schema='silver',
        unique_key='provider_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    provider_id,
    npi,
    tax_id,
    license_number,
    dea_number,
    provider_name,
    provider_last_name,
    provider_first_name,
    provider_middle_name,
    credential,
    provider_type_code,
    provider_type,
    entity_type_code,
    entity_type,
    taxonomy_code,
    provider_status_code,
    provider_status,
    effective_date,
    termination_date,
    is_valid_npi,
    updated_at

from {{ ref('stg_qnxt__provider') }}

{% endsnapshot %}
